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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

#include "arrow/flight/internal.h"
#include "arrow/flight/types.h"
#include "arrow/io/interfaces.h"
#include "arrow/ipc/message.h"
#include "arrow/result.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/cancel.h"
#include "arrow/util/future.h"

namespace arrow {

class Buffer;

namespace flight {
namespace internal {
/// Internal, not user-visible type used for memory-efficient reads from gRPC
/// stream
struct FlightData {
  /// Used only for puts, may be null
  std::shared_ptr<FlightDescriptor> descriptor;

  /// Non-length-prefixed Message header as described in format/Message.fbs
  std::shared_ptr<Buffer> metadata;

  /// Application-defined metadata
  std::shared_ptr<Buffer> app_metadata;

  /// Message body
  std::shared_ptr<Buffer> body;

  /// Open IPC message from the metadata and body
  ::arrow::Result<std::unique_ptr<ipc::Message>> OpenMessage();
};
}  // namespace internal
}  // namespace flight

template <>
struct IterationTraits<flight::internal::FlightData> {
  static flight::internal::FlightData End() {
    return flight::internal::FlightData{nullptr, nullptr, nullptr, nullptr};
  }
  static bool IsEnd(const flight::internal::FlightData& val) {
    return !val.descriptor && !val.metadata && !val.app_metadata && !val.body;
  }
};

namespace flight {
namespace internal {

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
bool ReadPayload(grpc::ClientReader<pb::FlightData>* reader, Future<FlightData>* data);
bool ReadPayload(grpc::ClientReaderWriter<pb::FlightData, pb::FlightData>* reader,
                 Future<FlightData>* data);
bool ReadPayload(grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* reader,
                 Future<FlightData>* data);
bool ReadPayload(grpc::ServerReaderWriter<pb::FlightData, pb::FlightData>* reader,
                 Future<FlightData>* data);
// Overload to make genericity easier in DoPutPayloadWriter
bool ReadPayload(grpc::ClientReaderWriter<pb::FlightData, pb::PutResult>* reader,
                 pb::PutResult* data);

template <typename ReaderPtr>
class FlightDataGenerator {
 public:
  FlightDataGenerator(ReaderPtr reader, std::shared_ptr<std::mutex> read_mutex)
      : reader_(reader), read_mutex_(std::move(read_mutex)), finished_(false) {}
  Future<FlightData> operator()() {
    if (finished_) {
      return Future<FlightData>::MakeFinished(IterationTraits<FlightData>::End());
    }
    auto guard = read_mutex_ ? std::unique_lock<std::mutex>(*read_mutex_)
                             : std::unique_lock<std::mutex>();
    // Ideally we'd make this cancellable too (Finish() should TryCancel() though that may
    // erase the actual error message)
    Future<FlightData> out;
    if (!ReadPayload(&*reader_, &out)) {
      finished_ = true;
      return Future<FlightData>::MakeFinished(IterationTraits<FlightData>::End());
    }
    return out;
  }

 private:
  ReaderPtr reader_;
  std::shared_ptr<std::mutex> read_mutex_;
  bool finished_;
};

template <typename T>
AsyncGenerator<FlightData> MakeFlightDataGenerator(
    T stream, std::shared_ptr<std::mutex> read_mutex) {
  return FlightDataGenerator<T>(stream, std::move(read_mutex));
}

template <typename T>
class BackgroundReadaheadGenerator {
 public:
  ARROW_DISALLOW_COPY_AND_ASSIGN(BackgroundReadaheadGenerator);

  explicit BackgroundReadaheadGenerator(io::IOContext io_context)
      : io_context_(io_context) {
    auto finished = std::make_shared<bool>(false);
    auto mutex = std::make_shared<std::mutex>();
    auto cv = std::make_shared<std::condition_variable>();
    auto queue = std::make_shared<std::queue<arrow::Future<T>>>();
    mark_finished_if_done_ = [mutex, cv, finished,
                              queue](const arrow::Result<T>& next_result) {
      if (!next_result.ok() || IsIterationEnd(*next_result)) {
        {
          std::unique_lock<std::mutex> lock(*mutex);
          *finished = true;
        }
        cv->notify_all();
      }
    };
    mutex_ = mutex;
    cv_ = cv;
    finished_ = std::move(finished);
    readahead_queue_ = queue;
  }

  ~BackgroundReadaheadGenerator() {
    {
      std::unique_lock<std::mutex> lock(*mutex_);
      *finished_ = true;
    }
    cv_->notify_all();
  }

  arrow::Result<Future<>> Start(arrow::AsyncGenerator<T> source_generator,
                                size_t max_readahead) {
    static std::atomic<int> counter;
    auto counter_val = counter.fetch_add(1);
    auto stop_token = io_context_.stop_token();
    auto finished = finished_;
    auto mutex = mutex_;
    auto cv = cv_;
    auto queue = readahead_queue_;
    auto mark_finished_if_done = mark_finished_if_done_;
    return io_context_.executor()->Submit([source_generator, mark_finished_if_done, mutex,
                                           cv, finished, queue, max_readahead, stop_token,
                                           counter_val]() mutable -> Status {
      std::cout << "VAL:" << counter_val << std::endl;
      while (!(*finished) && !stop_token.IsStopRequested()) {
        auto next = source_generator();
        next.AddCallback(mark_finished_if_done);
        {
          std::unique_lock<std::mutex> lock(*mutex);
          // Wait for capacity
          cv->wait(lock, [&] {
            return *finished || stop_token.IsStopRequested() ||
                   queue->size() < max_readahead;
          });
          if (stop_token.IsStopRequested() || *finished) {
            return Status::OK();
          }
          queue->push(std::move(next));
        }
        cv->notify_all();
      }
      return Status::OK();
    });
  }

  arrow::Future<T> operator()() {
    // Wait for an item
    auto stop_token = io_context_.stop_token();
    while (!(*finished_ && readahead_queue_->size() == 0) &&
           !stop_token.IsStopRequested()) {
      std::unique_lock<std::mutex> lock(*mutex_);
      cv_->wait(lock, [this, &stop_token] {
        return *finished_ || stop_token.IsStopRequested() || readahead_queue_->size() > 0;
      });
      if (stop_token.IsStopRequested() || (*finished_ && readahead_queue_->size() == 0)) {
        return arrow::Future<T>::MakeFinished(arrow::IterationTraits<T>::End());
      }
      auto result = readahead_queue_->front();
      readahead_queue_->pop();
      lock.unlock();
      cv_->notify_all();
      return result;
    }
    return arrow::Future<T>::MakeFinished(arrow::IterationTraits<T>::End());
  }

 private:
  io::IOContext io_context_;
  std::function<void(const arrow::Result<T>&)> mark_finished_if_done_;
  std::shared_ptr<std::mutex> mutex_;
  std::shared_ptr<std::condition_variable> cv_;
  std::shared_ptr<bool> finished_;
  std::shared_ptr<std::queue<arrow::Future<T>>> readahead_queue_;
};

// We want to reuse RecordBatchStreamReader's implementation while
// (1) Adapting it to the Flight message format
// (2) Allowing pure-metadata messages before data is sent
// (3) Reusing the reader implementation between DoGet and DoExchange.
// To do this, we wrap the gRPC reader in a peekable iterator.
// The Flight reader can then peek at the message to determine whether
// it has application metadata or not, and pass the message to
// RecordBatchStreamReader as appropriate.
class PeekableFlightDataReader {
 public:
  explicit PeekableFlightDataReader(AsyncGenerator<FlightData> gen,
                                    std::shared_ptr<std::mutex> read_mutex,
                                    Future<> finish)
      : gen_(gen), peek_(), finished_(false), valid_(false), finish_(finish) {}

  Status Peek(internal::FlightData** out) {
    *out = nullptr;
    if (finished_) {
      return Status::OK();
    }
    ARROW_ASSIGN_OR_RAISE(auto peeked, EnsurePeek());
    if (peeked) {
      *out = &peek_;
    }
    return Status::OK();
  }

  Status Next(internal::FlightData** out) {
    RETURN_NOT_OK(Peek(out));
    valid_ = false;
    return Status::OK();
  }

  /// \brief Peek() until the first data message.
  ///
  /// After this is called, either this will return \a false, or the
  /// next result of \a Peek and \a Next will contain Arrow data.
  arrow::Result<bool> SkipToData() {
    internal::FlightData* data;
    while (true) {
      RETURN_NOT_OK(Peek(&data));
      if (!data) {
        return false;
      }
      if (data->metadata) {
        return true;
      }
      RETURN_NOT_OK(Next(&data));
    }
  }

  Future<> WaitForClose() { return finish_; }

 private:
  arrow::Result<bool> EnsurePeek() {
    if (finished_ || valid_) {
      return valid_;
    }

    auto fut = gen_();
    auto result = fut.result();
    if (!result.ok()) {
      finished_ = true;
      valid_ = false;
      return result.status();
    }
    peek_ = result.ValueOrDie();
    if (IsIterationEnd(peek_)) {
      finished_ = true;
      valid_ = false;
    } else {
      valid_ = true;
    }
    return valid_;
  }

  AsyncGenerator<FlightData> gen_;
  internal::FlightData peek_;
  bool finished_;
  bool valid_;
  Future<> finish_;
};

template <typename ReaderPtr>
arrow::Result<std::shared_ptr<PeekableFlightDataReader>> MakePeekable(
    ReaderPtr stream, std::shared_ptr<std::mutex> read_mutex, int readahead,
    io::IOContext io_context = io::IOContext()) {
  auto gen = MakeFlightDataGenerator(stream, std::move(read_mutex));
  Future<> fut = Future<>::MakeFinished(Status::OK());
  if (readahead > 0) {
    auto bg_gen = std::make_shared<BackgroundReadaheadGenerator<FlightData>>(io_context);
    ARROW_ASSIGN_OR_RAISE(fut, bg_gen->Start(gen, readahead));
    gen = [bg_gen]() { return (*bg_gen)(); };
  }
  return std::make_shared<PeekableFlightDataReader>(gen, read_mutex, fut);
}

}  // namespace internal
}  // namespace flight
}  // namespace arrow
