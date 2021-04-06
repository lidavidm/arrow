// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "parquet/file_reader.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "arrow/io/caching.h"
#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"
#include "parquet/column_reader.h"
#include "parquet/column_scanner.h"
#include "parquet/encryption/encryption_internal.h"
#include "parquet/encryption/internal_file_decryptor.h"
#include "parquet/exception.h"
#include "parquet/file_writer.h"
#include "parquet/metadata.h"
#include "parquet/platform.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace parquet {

// PARQUET-978: Minimize footer reads by reading 64 KB from the end of the file
static constexpr int64_t kDefaultFooterReadSize = 64 * 1024;
static constexpr uint32_t kFooterSize = 8;

// For PARQUET-816
static constexpr int64_t kMaxDictHeaderSize = 100;

// ----------------------------------------------------------------------
// RowGroupReader public API

RowGroupReader::RowGroupReader(std::unique_ptr<Contents> contents)
    : contents_(std::move(contents)) {}

std::shared_ptr<ColumnReader> RowGroupReader::Column(int i) {
  if (i >= metadata()->num_columns()) {
    std::stringstream ss;
    ss << "Trying to read column index " << i << " but row group metadata has only "
       << metadata()->num_columns() << " columns";
    throw ParquetException(ss.str());
  }
  const ColumnDescriptor* descr = metadata()->schema()->Column(i);

  std::unique_ptr<PageReader> page_reader = contents_->GetColumnPageReader(i);
  return ColumnReader::Make(
      descr, std::move(page_reader),
      const_cast<ReaderProperties*>(contents_->properties())->memory_pool());
}

std::unique_ptr<PageReader> RowGroupReader::GetColumnPageReader(int i) {
  if (i >= metadata()->num_columns()) {
    std::stringstream ss;
    ss << "Trying to read column index " << i << " but row group metadata has only "
       << metadata()->num_columns() << " columns";
    throw ParquetException(ss.str());
  }
  return contents_->GetColumnPageReader(i);
}

// Returns the rowgroup metadata
const RowGroupMetaData* RowGroupReader::metadata() const { return contents_->metadata(); }

/// Compute the section of the file that should be read for the given
/// row group and column chunk.
::arrow::io::ReadRange ComputeColumnChunkRange(FileMetaData* file_metadata,
                                               int64_t source_size, int row_group_index,
                                               int column_index) {
  auto row_group_metadata = file_metadata->RowGroup(row_group_index);
  auto column_metadata = row_group_metadata->ColumnChunk(column_index);

  int64_t col_start = column_metadata->data_page_offset();
  if (column_metadata->has_dictionary_page() &&
      column_metadata->dictionary_page_offset() > 0 &&
      col_start > column_metadata->dictionary_page_offset()) {
    col_start = column_metadata->dictionary_page_offset();
  }

  int64_t col_length = column_metadata->total_compressed_size();
  // PARQUET-816 workaround for old files created by older parquet-mr
  const ApplicationVersion& version = file_metadata->writer_version();
  if (version.VersionLt(ApplicationVersion::PARQUET_816_FIXED_VERSION())) {
    // The Parquet MR writer had a bug in 1.2.8 and below where it didn't include the
    // dictionary page header size in total_compressed_size and total_uncompressed_size
    // (see IMPALA-694). We add padding to compensate.
    int64_t bytes_remaining = source_size - (col_start + col_length);
    int64_t padding = std::min<int64_t>(kMaxDictHeaderSize, bytes_remaining);
    col_length += padding;
  }

  return {col_start, col_length};
}

// RowGroupReader::Contents implementation for the Parquet file specification
class SerializedRowGroup : public RowGroupReader::Contents {
 public:
  SerializedRowGroup(std::shared_ptr<ArrowInputFile> source,
                     std::shared_ptr<::arrow::io::internal::ReadRangeCache> cached_source,
                     int64_t source_size, FileMetaData* file_metadata,
                     int row_group_number, const ReaderProperties& props,
                     std::shared_ptr<InternalFileDecryptor> file_decryptor = nullptr)
      : source_(std::move(source)),
        cached_source_(std::move(cached_source)),
        source_size_(source_size),
        file_metadata_(file_metadata),
        properties_(props),
        row_group_ordinal_(row_group_number),
        file_decryptor_(file_decryptor) {
    row_group_metadata_ = file_metadata->RowGroup(row_group_number);
  }

  const RowGroupMetaData* metadata() const override { return row_group_metadata_.get(); }

  const ReaderProperties* properties() const override { return &properties_; }

  std::unique_ptr<PageReader> GetColumnPageReader(int i) override {
    // Read column chunk from the file
    auto col = row_group_metadata_->ColumnChunk(i);

    ::arrow::io::ReadRange col_range =
        ComputeColumnChunkRange(file_metadata_, source_size_, row_group_ordinal_, i);
    std::shared_ptr<ArrowInputStream> stream;
    if (cached_source_) {
      // PARQUET-1698: if read coalescing is enabled, read from pre-buffered
      // segments.
      PARQUET_ASSIGN_OR_THROW(auto buffer, cached_source_->Read(col_range));
      stream = std::make_shared<::arrow::io::BufferReader>(buffer);
    } else {
      stream = properties_.GetStream(source_, col_range.offset, col_range.length);
    }

    std::unique_ptr<ColumnCryptoMetaData> crypto_metadata = col->crypto_metadata();

    // Column is encrypted only if crypto_metadata exists.
    if (!crypto_metadata) {
      return PageReader::Open(stream, col->num_values(), col->compression(),
                              properties_.memory_pool());
    }

    if (file_decryptor_ == nullptr) {
      throw ParquetException("RowGroup is noted as encrypted but no file decryptor");
    }

    constexpr auto kEncryptedRowGroupsLimit = 32767;
    if (i > kEncryptedRowGroupsLimit) {
      throw ParquetException("Encrypted files cannot contain more than 32767 row groups");
    }

    // The column is encrypted
    std::shared_ptr<Decryptor> meta_decryptor;
    std::shared_ptr<Decryptor> data_decryptor;
    // The column is encrypted with footer key
    if (crypto_metadata->encrypted_with_footer_key()) {
      meta_decryptor = file_decryptor_->GetFooterDecryptorForColumnMeta();
      data_decryptor = file_decryptor_->GetFooterDecryptorForColumnData();
      CryptoContext ctx(col->has_dictionary_page(), row_group_ordinal_,
                        static_cast<int16_t>(i), meta_decryptor, data_decryptor);
      return PageReader::Open(stream, col->num_values(), col->compression(),
                              properties_.memory_pool(), &ctx);
    }

    // The column is encrypted with its own key
    std::string column_key_metadata = crypto_metadata->key_metadata();
    const std::string column_path = crypto_metadata->path_in_schema()->ToDotString();

    meta_decryptor =
        file_decryptor_->GetColumnMetaDecryptor(column_path, column_key_metadata);
    data_decryptor =
        file_decryptor_->GetColumnDataDecryptor(column_path, column_key_metadata);

    CryptoContext ctx(col->has_dictionary_page(), row_group_ordinal_,
                      static_cast<int16_t>(i), meta_decryptor, data_decryptor);
    return PageReader::Open(stream, col->num_values(), col->compression(),
                            properties_.memory_pool(), &ctx);
  }

 private:
  std::shared_ptr<ArrowInputFile> source_;
  // Will be nullptr if PreBuffer() is not called.
  std::shared_ptr<::arrow::io::internal::ReadRangeCache> cached_source_;
  int64_t source_size_;
  FileMetaData* file_metadata_;
  std::unique_ptr<RowGroupMetaData> row_group_metadata_;
  ReaderProperties properties_;
  int row_group_ordinal_;
  std::shared_ptr<InternalFileDecryptor> file_decryptor_;
};

// ----------------------------------------------------------------------
// SerializedFile: An implementation of ParquetFileReader::Contents that deals
// with the Parquet file structure, Thrift deserialization, and other internal
// matters

// This class takes ownership of the provided data source
class SerializedFile : public ParquetFileReader::Contents {
 public:
  SerializedFile(std::shared_ptr<ArrowInputFile> source,
                 const ReaderProperties& props = default_reader_properties())
      : source_(std::move(source)), properties_(props) {
    PARQUET_ASSIGN_OR_THROW(source_size_, source_->GetSize());
  }

  ~SerializedFile() override {
    try {
      Close();
    } catch (...) {
    }
  }

  void Close() override {
    if (file_decryptor_) file_decryptor_->WipeOutDecryptionKeys();
  }

  std::shared_ptr<RowGroupReader> GetRowGroup(int i) override {
    std::unique_ptr<SerializedRowGroup> contents(
        new SerializedRowGroup(source_, cached_source_, source_size_,
                               file_metadata_.get(), i, properties_, file_decryptor_));
    return std::make_shared<RowGroupReader>(std::move(contents));
  }

  std::shared_ptr<FileMetaData> metadata() const override { return file_metadata_; }

  void set_metadata(std::shared_ptr<FileMetaData> metadata) {
    file_metadata_ = std::move(metadata);
  }

  ::arrow::Future<> PreBuffer(const std::vector<int>& row_groups,
                              const std::vector<int>& column_indices,
                              const ::arrow::io::IOContext& ctx,
                              const ::arrow::io::CacheOptions& options) {
    cached_source_ =
        std::make_shared<::arrow::io::internal::ReadRangeCache>(source_, ctx, options);
    std::vector<::arrow::io::ReadRange> ranges;
    for (int row : row_groups) {
      for (int col : column_indices) {
        ranges.push_back(
            ComputeColumnChunkRange(file_metadata_.get(), source_size_, row, col));
      }
    }
    PARQUET_THROW_NOT_OK(cached_source_->Cache(ranges));
    return cached_source_->Wait();
  }

  ::arrow::Future<> WhenBuffered(const std::vector<int>& row_groups,
                                 const std::vector<int>& column_indices) const {
    if (!cached_source_) {
      throw ParquetException("Must call PreBuffer before WhenBuffered");
    }
    std::vector<::arrow::io::ReadRange> ranges;
    for (int row : row_groups) {
      for (int col : column_indices) {
        ranges.push_back(
            ComputeColumnChunkRange(file_metadata_.get(), source_size_, row, col));
      }
    }
    return cached_source_->WaitFor(ranges);
  }

  ::arrow::Future<> ParseMetaData() {
    if (source_size_ == 0) {
      throw ParquetInvalidOrCorruptedFileException("Parquet file size is 0 bytes");
    } else if (source_size_ < kFooterSize) {
      throw ParquetInvalidOrCorruptedFileException(
          "Parquet file size is ", source_size_,
          " bytes, smaller than the minimum file footer (", kFooterSize, " bytes)");
    }

    int64_t footer_read_size = std::min(source_size_, kDefaultFooterReadSize);
    return source_->ReadAsync(source_size_ - footer_read_size, footer_read_size)
        .Then([=](const std::shared_ptr<::arrow::Buffer>& footer_buffer)
                  -> ::arrow::Future<> {
          // Check if all bytes are read. Check if last 4 bytes read have the magic bits
          if (footer_buffer->size() != footer_read_size ||
              (memcmp(footer_buffer->data() + footer_read_size - 4, kParquetMagic, 4) !=
                   0 &&
               memcmp(footer_buffer->data() + footer_read_size - 4, kParquetEMagic, 4) !=
                   0)) {
            return ::arrow::Status::FromDetailAndArgs(
                ::arrow::StatusCode::IOError,
                ParquetInvalidOrCorruptedFileStatusDetail::Instance(),
                "Parquet magic bytes not found in footer. Either the file is corrupted "
                "or this is not a parquet file.");
          }

          if (memcmp(footer_buffer->data() + footer_read_size - 4, kParquetEMagic, 4) ==
              0) {
            // Encrypted file with Encrypted footer.
            BEGIN_PARQUET_CATCH_EXCEPTIONS
            return ParseMetaDataOfEncryptedFileWithEncryptedFooter(footer_buffer,
                                                                   footer_read_size);
            END_PARQUET_CATCH_EXCEPTIONS
          }

          // No encryption or encryption with plaintext footer mode.
          BEGIN_PARQUET_CATCH_EXCEPTIONS
          return ParseUnencryptedFileMetadata(footer_buffer, footer_read_size)
              .Then([=](const MetadataBuffer& metadata_buffer) -> ::arrow::Status {
                auto file_decryption_properties =
                    properties_.file_decryption_properties().get();
                if (!file_metadata_
                         ->is_encryption_algorithm_set()) {  // Non encrypted file.
                  if (file_decryption_properties != nullptr) {
                    if (!file_decryption_properties->plaintext_files_allowed()) {
                      return ::arrow::Status::Invalid(
                          "Applying decryption properties on plaintext file");
                    }
                  }
                } else {
                  // Encrypted file with plaintext footer mode.
                  BEGIN_PARQUET_CATCH_EXCEPTIONS
                  ParseMetaDataOfEncryptedFileWithPlaintextFooter(
                      file_decryption_properties, metadata_buffer);
                  END_PARQUET_CATCH_EXCEPTIONS
                }
                return ::arrow::Status::OK();
              });
          END_PARQUET_CATCH_EXCEPTIONS
        });
  }

 private:
  std::shared_ptr<ArrowInputFile> source_;
  std::shared_ptr<::arrow::io::internal::ReadRangeCache> cached_source_;
  int64_t source_size_;
  std::shared_ptr<FileMetaData> file_metadata_;
  ReaderProperties properties_;

  std::shared_ptr<InternalFileDecryptor> file_decryptor_;

  struct MetadataBuffer {
    std::shared_ptr<Buffer> buffer;
    uint32_t metadata_len;
    uint32_t read_metadata_len;
  };

  ::arrow::Future<MetadataBuffer> ParseUnencryptedFileMetadata(
      const std::shared_ptr<Buffer>& footer_buffer, int64_t footer_read_size);

  std::string HandleAadPrefix(FileDecryptionProperties* file_decryption_properties,
                              EncryptionAlgorithm& algo);

  void ParseMetaDataOfEncryptedFileWithPlaintextFooter(
      FileDecryptionProperties* file_decryption_properties,
      const MetadataBuffer& metadata_buffer);

  ::arrow::Future<> ParseMetaDataOfEncryptedFileWithEncryptedFooter(
      const std::shared_ptr<Buffer>& footer_buffer, int64_t footer_read_size);
};

uint32_t CheckFooterLength(const std::shared_ptr<Buffer>& footer_buffer,
                           int64_t footer_read_size, int64_t source_size) {
  uint32_t footer_len = ::arrow::util::SafeLoadAs<uint32_t>(
      reinterpret_cast<const uint8_t*>(footer_buffer->data()) + footer_read_size -
      kFooterSize);
  if (kFooterSize + footer_len > source_size) {
    throw ParquetInvalidOrCorruptedFileException(
        "Parquet file size is ", source_size,
        " bytes, smaller than the size reported by footer's (", footer_len, "bytes)");
  }
  return footer_len;
}

::arrow::Future<SerializedFile::MetadataBuffer>
SerializedFile::ParseUnencryptedFileMetadata(const std::shared_ptr<Buffer>& footer_buffer,
                                             int64_t footer_read_size) {
  uint32_t metadata_len =
      CheckFooterLength(footer_buffer, footer_read_size, source_size_);
  int64_t metadata_start = source_size_ - kFooterSize - metadata_len;

  // Check if the footer_buffer contains the entire metadata
  ::arrow::Future<std::shared_ptr<::arrow::Buffer>> buffer_fut;
  if (footer_read_size >= (metadata_len + kFooterSize)) {
    buffer_fut =
        ::arrow::Future<std::shared_ptr<::arrow::Buffer>>::MakeFinished(SliceBuffer(
            footer_buffer, footer_read_size - metadata_len - kFooterSize, metadata_len));
  } else {
    buffer_fut = source_->ReadAsync(metadata_start, metadata_len);
  }

  return buffer_fut.Then([=](const std::shared_ptr<::arrow::Buffer>& metadata_buffer)
                             -> ::arrow::Result<SerializedFile::MetadataBuffer> {
    if (metadata_buffer->size() != metadata_len) {
      return ::arrow::Status::Invalid(
          "Failed reading metadata buffer (requested " + std::to_string(metadata_len) +
          " bytes but got " + std::to_string(metadata_buffer->size()) + " bytes)");
    }
    uint32_t read_metadata_len = metadata_len;
    BEGIN_PARQUET_CATCH_EXCEPTIONS
    file_metadata_ = FileMetaData::Make(metadata_buffer->data(), &read_metadata_len);
    END_PARQUET_CATCH_EXCEPTIONS
    return SerializedFile::MetadataBuffer{metadata_buffer, metadata_len,
                                          read_metadata_len};
  });
}

::arrow::Future<> SerializedFile::ParseMetaDataOfEncryptedFileWithEncryptedFooter(
    const std::shared_ptr<Buffer>& footer_buffer, int64_t footer_read_size) {
  // encryption with encrypted footer
  // both metadata & crypto metadata length
  uint32_t footer_len = CheckFooterLength(footer_buffer, footer_read_size, source_size_);
  int64_t crypto_metadata_start = source_size_ - kFooterSize - footer_len;
  // Check if the footer_buffer contains the entire metadata
  ::arrow::Future<std::shared_ptr<::arrow::Buffer>> buffer_fut;
  if (footer_read_size >= (footer_len + kFooterSize)) {
    buffer_fut =
        ::arrow::Future<std::shared_ptr<::arrow::Buffer>>::MakeFinished(SliceBuffer(
            footer_buffer, footer_read_size - footer_len - kFooterSize, footer_len));
  } else {
    buffer_fut = source_->ReadAsync(crypto_metadata_start, footer_len);
  }
  return buffer_fut.Then([=](std::shared_ptr<::arrow::Buffer> crypto_metadata_buffer)
                             -> ::arrow::Future<> {
    if (crypto_metadata_buffer->size() != footer_len) {
      return ::arrow::Status::Invalid(
          "Failed reading encrypted metadata buffer (requested " +
          std::to_string(footer_len) + " bytes but got " +
          std::to_string(crypto_metadata_buffer->size()) + " bytes)");
    }
    auto file_decryption_properties = properties_.file_decryption_properties().get();
    if (file_decryption_properties == nullptr) {
      return ::arrow::Status::Invalid(
          "Could not read encrypted metadata, no decryption found in reader's "
          "properties");
    }
    uint32_t crypto_metadata_len = footer_len;
    std::shared_ptr<FileCryptoMetaData> file_crypto_metadata;
    BEGIN_PARQUET_CATCH_EXCEPTIONS
    file_crypto_metadata =
        FileCryptoMetaData::Make(crypto_metadata_buffer->data(), &crypto_metadata_len);
    // Handle AAD prefix
    EncryptionAlgorithm algo = file_crypto_metadata->encryption_algorithm();
    std::string file_aad = HandleAadPrefix(file_decryption_properties, algo);
    file_decryptor_ = std::make_shared<InternalFileDecryptor>(
        file_decryption_properties, file_aad, algo.algorithm,
        file_crypto_metadata->key_metadata(), properties_.memory_pool());
    END_PARQUET_CATCH_EXCEPTIONS

    int64_t metadata_offset =
        source_size_ - kFooterSize - footer_len + crypto_metadata_len;
    uint32_t metadata_len = footer_len - crypto_metadata_len;
    return source_->ReadAsync(metadata_offset, metadata_len)
        .Then([=](std::shared_ptr<::arrow::Buffer> metadata_buffer) -> ::arrow::Status {
          if (metadata_buffer->size() != metadata_len) {
            return ::arrow::Status::Invalid(
                "Failed reading metadata buffer (requested " +
                std::to_string(metadata_len) + " bytes but got " +
                std::to_string(metadata_buffer->size()) + " bytes)");
          }
          uint32_t mut_metadata_len = metadata_len;
          BEGIN_PARQUET_CATCH_EXCEPTIONS
          file_metadata_ = FileMetaData::Make(metadata_buffer->data(), &mut_metadata_len,
                                              file_decryptor_);
          END_PARQUET_CATCH_EXCEPTIONS
          return ::arrow::Status::OK();
        });
  });
}

void SerializedFile::ParseMetaDataOfEncryptedFileWithPlaintextFooter(
    FileDecryptionProperties* file_decryption_properties,
    const MetadataBuffer& metadata_buffer) {
  // Providing decryption properties in plaintext footer mode is not mandatory, for
  // example when reading by legacy reader.
  if (file_decryption_properties != nullptr) {
    EncryptionAlgorithm algo = file_metadata_->encryption_algorithm();
    // Handle AAD prefix
    std::string file_aad = HandleAadPrefix(file_decryption_properties, algo);
    file_decryptor_ = std::make_shared<InternalFileDecryptor>(
        file_decryption_properties, file_aad, algo.algorithm,
        file_metadata_->footer_signing_key_metadata(), properties_.memory_pool());
    // set the InternalFileDecryptor in the metadata as well, as it's used
    // for signature verification and for ColumnChunkMetaData creation.
    file_metadata_->set_file_decryptor(file_decryptor_);

    if (file_decryption_properties->check_plaintext_footer_integrity()) {
      if (metadata_buffer.metadata_len - metadata_buffer.read_metadata_len !=
          (parquet::encryption::kGcmTagLength + parquet::encryption::kNonceLength)) {
        throw ParquetInvalidOrCorruptedFileException(
            "Failed reading metadata for encryption signature (requested ",
            parquet::encryption::kGcmTagLength + parquet::encryption::kNonceLength,
            " bytes but have ",
            metadata_buffer.metadata_len - metadata_buffer.read_metadata_len, " bytes)");
      }

      if (!file_metadata_->VerifySignature(metadata_buffer.buffer->data() +
                                           metadata_buffer.read_metadata_len)) {
        throw ParquetInvalidOrCorruptedFileException(
            "Parquet crypto signature verification failed");
      }
    }
  }
}

std::string SerializedFile::HandleAadPrefix(
    FileDecryptionProperties* file_decryption_properties, EncryptionAlgorithm& algo) {
  std::string aad_prefix_in_properties = file_decryption_properties->aad_prefix();
  std::string aad_prefix = aad_prefix_in_properties;
  bool file_has_aad_prefix = algo.aad.aad_prefix.empty() ? false : true;
  std::string aad_prefix_in_file = algo.aad.aad_prefix;

  if (algo.aad.supply_aad_prefix && aad_prefix_in_properties.empty()) {
    throw ParquetException(
        "AAD prefix used for file encryption, "
        "but not stored in file and not supplied "
        "in decryption properties");
  }

  if (file_has_aad_prefix) {
    if (!aad_prefix_in_properties.empty()) {
      if (aad_prefix_in_properties.compare(aad_prefix_in_file) != 0) {
        throw ParquetException(
            "AAD Prefix in file and in properties "
            "is not the same");
      }
    }
    aad_prefix = aad_prefix_in_file;
    std::shared_ptr<AADPrefixVerifier> aad_prefix_verifier =
        file_decryption_properties->aad_prefix_verifier();
    if (aad_prefix_verifier != nullptr) aad_prefix_verifier->Verify(aad_prefix);
  } else {
    if (!algo.aad.supply_aad_prefix && !aad_prefix_in_properties.empty()) {
      throw ParquetException(
          "AAD Prefix set in decryption properties, but was not used "
          "for file encryption");
    }
    std::shared_ptr<AADPrefixVerifier> aad_prefix_verifier =
        file_decryption_properties->aad_prefix_verifier();
    if (aad_prefix_verifier != nullptr) {
      throw ParquetException(
          "AAD Prefix Verifier is set, but AAD Prefix not found in file");
    }
  }
  return aad_prefix + algo.aad.aad_file_unique;
}

// ----------------------------------------------------------------------
// ParquetFileReader public API

ParquetFileReader::ParquetFileReader() {}

ParquetFileReader::~ParquetFileReader() {
  try {
    Close();
  } catch (...) {
  }
}

// Open the file. If no metadata is passed, it is parsed from the footer of
// the file
std::unique_ptr<ParquetFileReader::Contents> ParquetFileReader::Contents::Open(
    std::shared_ptr<ArrowInputFile> source, const ReaderProperties& props,
    std::shared_ptr<FileMetaData> metadata) {
  std::unique_ptr<ParquetFileReader::Contents> result(
      new SerializedFile(std::move(source), props));

  // Access private methods here, but otherwise unavailable
  SerializedFile* file = static_cast<SerializedFile*>(result.get());

  if (metadata == nullptr) {
    // Validates magic bytes, parses metadata, and initializes the SchemaDescriptor
    auto fut = file->ParseMetaData();
    PARQUET_THROW_NOT_OK(fut.status());
  } else {
    file->set_metadata(std::move(metadata));
  }

  return result;
}

::arrow::Future<std::unique_ptr<ParquetFileReader::Contents>>
ParquetFileReader::Contents::OpenAsync(std::shared_ptr<ArrowInputFile> source,
                                       const ReaderProperties& props,
                                       std::shared_ptr<FileMetaData> metadata) {
  BEGIN_PARQUET_CATCH_EXCEPTIONS
  std::unique_ptr<ParquetFileReader::Contents> result(
      new SerializedFile(std::move(source), props));
  SerializedFile* file = static_cast<SerializedFile*>(result.get());
  if (metadata == nullptr) {
    struct {
      ::arrow::Result<std::unique_ptr<ParquetFileReader::Contents>> operator()(
          ::arrow::Result<::arrow::detail::Empty>) {
        return std::move(result);
      }

      std::unique_ptr<ParquetFileReader::Contents> result;
    } Continuation;
    Continuation.result = std::move(result);
    return file->ParseMetaData().Then(std::move(Continuation));
  } else {
    file->set_metadata(std::move(metadata));
    return ::arrow::Future<std::unique_ptr<ParquetFileReader::Contents>>::MakeFinished(
        std::move(result));
  }
  END_PARQUET_CATCH_EXCEPTIONS
}

std::unique_ptr<ParquetFileReader> ParquetFileReader::Open(
    std::shared_ptr<::arrow::io::RandomAccessFile> source, const ReaderProperties& props,
    std::shared_ptr<FileMetaData> metadata) {
  auto contents = SerializedFile::Open(std::move(source), props, std::move(metadata));
  std::unique_ptr<ParquetFileReader> result(new ParquetFileReader());
  result->Open(std::move(contents));
  return result;
}

std::unique_ptr<ParquetFileReader> ParquetFileReader::OpenFile(
    const std::string& path, bool memory_map, const ReaderProperties& props,
    std::shared_ptr<FileMetaData> metadata) {
  std::shared_ptr<::arrow::io::RandomAccessFile> source;
  if (memory_map) {
    PARQUET_ASSIGN_OR_THROW(
        source, ::arrow::io::MemoryMappedFile::Open(path, ::arrow::io::FileMode::READ));
  } else {
    PARQUET_ASSIGN_OR_THROW(source,
                            ::arrow::io::ReadableFile::Open(path, props.memory_pool()));
  }

  return Open(std::move(source), props, std::move(metadata));
}

// TODO: this is a massive pain to use as an API
::arrow::Future<std::unique_ptr<ParquetFileReader>> ParquetFileReader::OpenAsync(
    std::shared_ptr<::arrow::io::RandomAccessFile> source, const ReaderProperties& props,
    std::shared_ptr<FileMetaData> metadata) {
  BEGIN_PARQUET_CATCH_EXCEPTIONS
  auto fut = SerializedFile::OpenAsync(std::move(source), props, std::move(metadata));
  auto completed = ::arrow::Future<std::unique_ptr<ParquetFileReader>>::Make();
  fut.AddCallback([fut, completed](
                      const ::arrow::Result<std::unique_ptr<ParquetFileReader::Contents>>&
                          contents) mutable {
    if (!contents.ok()) {
      completed.MarkFinished(contents.status());
      return;
    }
    std::unique_ptr<ParquetFileReader> result(new ParquetFileReader());
    result->Open(fut.MoveResult().MoveValueUnsafe());
    completed.MarkFinished(std::move(result));
  });
  return completed;
  END_PARQUET_CATCH_EXCEPTIONS
}

void ParquetFileReader::Open(std::unique_ptr<ParquetFileReader::Contents> contents) {
  contents_ = std::move(contents);
}

void ParquetFileReader::Close() {
  if (contents_) {
    contents_->Close();
  }
}

std::shared_ptr<FileMetaData> ParquetFileReader::metadata() const {
  return contents_->metadata();
}

std::shared_ptr<RowGroupReader> ParquetFileReader::RowGroup(int i) {
  if (i >= metadata()->num_row_groups()) {
    std::stringstream ss;
    ss << "Trying to read row group " << i << " but file only has "
       << metadata()->num_row_groups() << " row groups";
    throw ParquetException(ss.str());
  }
  return contents_->GetRowGroup(i);
}

::arrow::Future<> ParquetFileReader::PreBuffer(const std::vector<int>& row_groups,
                                               const std::vector<int>& column_indices,
                                               const ::arrow::io::IOContext& ctx,
                                               const ::arrow::io::CacheOptions& options) {
  // Access private methods here
  SerializedFile* file =
      ::arrow::internal::checked_cast<SerializedFile*>(contents_.get());
  return file->PreBuffer(row_groups, column_indices, ctx, options);
}

::arrow::Future<> ParquetFileReader::WhenBuffered(
    const std::vector<int>& row_groups, const std::vector<int>& column_indices) const {
  // Access private methods here
  SerializedFile* file =
      ::arrow::internal::checked_cast<SerializedFile*>(contents_.get());
  return file->WhenBuffered(row_groups, column_indices);
}

// ----------------------------------------------------------------------
// File metadata helpers

std::shared_ptr<FileMetaData> ReadMetaData(
    const std::shared_ptr<::arrow::io::RandomAccessFile>& source) {
  return ParquetFileReader::Open(source)->metadata();
}

// ----------------------------------------------------------------------
// File scanner for performance testing

int64_t ScanFileContents(std::vector<int> columns, const int32_t column_batch_size,
                         ParquetFileReader* reader) {
  std::vector<int16_t> rep_levels(column_batch_size);
  std::vector<int16_t> def_levels(column_batch_size);

  int num_columns = static_cast<int>(columns.size());

  // columns are not specified explicitly. Add all columns
  if (columns.size() == 0) {
    num_columns = reader->metadata()->num_columns();
    columns.resize(num_columns);
    for (int i = 0; i < num_columns; i++) {
      columns[i] = i;
    }
  }

  std::vector<int64_t> total_rows(num_columns, 0);

  for (int r = 0; r < reader->metadata()->num_row_groups(); ++r) {
    auto group_reader = reader->RowGroup(r);
    int col = 0;
    for (auto i : columns) {
      std::shared_ptr<ColumnReader> col_reader = group_reader->Column(i);
      size_t value_byte_size = GetTypeByteSize(col_reader->descr()->physical_type());
      std::vector<uint8_t> values(column_batch_size * value_byte_size);

      int64_t values_read = 0;
      while (col_reader->HasNext()) {
        int64_t levels_read =
            ScanAllValues(column_batch_size, def_levels.data(), rep_levels.data(),
                          values.data(), &values_read, col_reader.get());
        if (col_reader->descr()->max_repetition_level() > 0) {
          for (int64_t i = 0; i < levels_read; i++) {
            if (rep_levels[i] == 0) {
              total_rows[col]++;
            }
          }
        } else {
          total_rows[col] += levels_read;
        }
      }
      col++;
    }
  }

  for (int i = 1; i < num_columns; ++i) {
    if (total_rows[0] != total_rows[i]) {
      throw ParquetException("Parquet error: Total rows among columns do not match");
    }
  }

  return total_rows[0];
}

}  // namespace parquet
