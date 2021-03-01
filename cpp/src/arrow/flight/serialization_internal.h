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

// (De)serialization utilities that hook into gRPC, efficiently
// handling Arrow-encoded data in a gRPC call.

#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>

#include "arrow/flight/internal.h"
#include "arrow/flight/types.h"
#include "arrow/io/interfaces.h"
#include "arrow/ipc/message.h"
#include "arrow/result.h"
#include "arrow/util/optional.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

class Buffer;

namespace flight {
namespace internal {

/// Internal, not user-visible type used for memory-efficient reads from gRPC
/// stream
struct FlightData {
  /// Used only for puts, may be null
  std::unique_ptr<FlightDescriptor> descriptor;

  /// Non-length-prefixed Message header as described in format/Message.fbs
  std::shared_ptr<Buffer> metadata;

  /// Application-defined metadata
  std::shared_ptr<Buffer> app_metadata;

  /// Message body
  std::shared_ptr<Buffer> body;

  /// Open IPC message from the metadata and body
  ::arrow::Result<std::unique_ptr<ipc::Message>> OpenMessage();
};

/// Write Flight message on gRPC stream with zero-copy optimizations.
/// True is returned on success, false if some error occurred (connection closed?).
bool WritePayload(const FlightPayload& payload,
                  grpc::ClientReaderWriter<pb::FlightData, pb::PutResult>* writer);
bool WritePayload(const FlightPayload& payload,
                  grpc::ClientReaderWriter<pb::FlightData, pb::FlightData>* writer);
bool WritePayload(const FlightPayload& payload,
                  grpc::ServerReaderWriter<pb::FlightData, pb::FlightData>* writer);
bool WritePayload(const FlightPayload& payload,
                  grpc::ServerWriter<pb::FlightData>* writer);

/// Read Flight message from gRPC stream with zero-copy optimizations.
/// True is returned on success, false if stream ended.
bool ReadPayload(grpc::ClientReader<pb::FlightData>* reader, FlightData* data);
bool ReadPayload(grpc::ClientReaderWriter<pb::FlightData, pb::FlightData>* reader,
                 FlightData* data);
bool ReadPayload(grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* reader,
                 FlightData* data);
bool ReadPayload(grpc::ServerReaderWriter<pb::FlightData, pb::FlightData>* reader,
                 FlightData* data);
// Overload to make genericity easier in DoPutPayloadWriter
bool ReadPayload(grpc::ClientReaderWriter<pb::FlightData, pb::PutResult>* reader,
                 pb::PutResult* data);

// We want to reuse RecordBatchStreamReader's implementation while
// (1) Adapting it to the Flight message format
// (2) Allowing pure-metadata messages before data is sent
// (3) Reusing the reader implementation between DoGet and DoExchange.
// To do this, we wrap the gRPC reader in a peekable iterator.
// The Flight reader can then peek at the message to determine whether
// it has application metadata or not, and pass the message to
// RecordBatchStreamReader as appropriate.
class FlightDataReader : public std::enable_shared_from_this<FlightDataReader> {
 public:
  virtual const FlightData* Peek() = 0;
  virtual util::optional<FlightData> Advance() = 0;
  virtual bool SkipToData() = 0;
  virtual void Drain() {
    while (Advance().has_value()) {}
  }
};

// TODO: move templates into .cc file and explicitly instantiate only the ones we need
template <typename ReaderPtr>
class SynchronousFlightDataReader : public FlightDataReader {
 public:
  explicit SynchronousFlightDataReader(ReaderPtr stream)
      : stream_(stream), peek_(), finished_(false), valid_(false) {}

  const FlightData* Peek() override {
    if (finished_) {
      return nullptr;
    } else if (EnsurePeek()) {
      return &peek_;
    }
    return nullptr;
  }

  util::optional<FlightData> Advance() override {
    if (finished_) {
      return util::nullopt;
    } else if (EnsurePeek()) {
      valid_ = false;
      return util::make_optional<FlightData>(std::move(peek_));
    }
    return util::nullopt;
  }

  /// \brief Peek() until the first data message.
  ///
  /// After this is called, either this will return \a false, or the
  /// next result of \a Peek and \a Next will contain Arrow data.
  bool SkipToData() override {
    while (true) {
      auto data = Peek();
      if (!data) {
        return false;
      }
      if (data->metadata) {
        return true;
      }
      valid_ = false;
    }
  }

 private:
  bool EnsurePeek() {
    if (finished_ || valid_) {
      return valid_;
    }

    if (!internal::ReadPayload(&*stream_, &peek_)) {
      finished_ = true;
      valid_ = false;
    } else {
      valid_ = true;
    }
    return valid_;
  }

  ReaderPtr stream_;
  internal::FlightData peek_;
  bool finished_;
  bool valid_;
};

template <typename ReaderPtr>
class ReadaheadFlightDataReader : public FlightDataReader {
 public:
  explicit ReadaheadFlightDataReader(ReaderPtr stream)
      : reader_(stream), queue_(), finished_(false), lock_(), cv_() {}

  Status Start(size_t readahead, std::shared_ptr<std::mutex> read_mutex,
               io::IOContext io_context) {
    auto self = std::dynamic_pointer_cast<ReadaheadFlightDataReader>(shared_from_this());
    return io_context.executor()->Spawn([=]() {
      while (true) {
        util::optional<FlightData> item;
        {
          // std::unique_lock<std::mutex> lock(*read_mutex);
          item = std::move(self->reader_.Advance());
        }
        std::unique_lock<std::mutex> lock(self->lock_);
        if (item.has_value()) {
          cv_.wait(lock, [=]() { return self->queue_.size() < readahead; });
          self->queue_.push(std::move(item.value()));
        } else {
          self->finished_ = true;
        }
        cv_.notify_all();
        if (self->finished_) break;
      }
    });
  }

  const FlightData* Peek() override {
    std::unique_lock<std::mutex> lock(lock_);
    cv_.wait(lock, [this]() { return !queue_.empty() || finished_; });
    return queue_.empty() ? nullptr : &queue_.front();
  }

  util::optional<FlightData> Advance() override {
    std::unique_lock<std::mutex> lock(lock_);
    cv_.wait(lock, [this]() { return !queue_.empty() || finished_; });
    if (queue_.empty()) {
      return util::nullopt;
    }
    bool before = queue_.front().body != nullptr;
    auto result = util::make_optional<FlightData>(std::move(queue_.front()));
    bool after = result.value().body != nullptr;
    queue_.pop();
    cv_.notify_all();
    return result;
  }

  bool SkipToData() override {
    while (true) {
      auto data = Peek();
      if (!data) {
        return false;
      }
      if (data->metadata) {
        return true;
      }
      Advance();
    }
  }

 private:
  SynchronousFlightDataReader<ReaderPtr> reader_;
  std::queue<FlightData> queue_;
  bool finished_;
  std::mutex lock_;
  std::condition_variable cv_;
};

}  // namespace internal
}  // namespace flight
}  // namespace arrow
