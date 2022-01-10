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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/flight/test_util.h"
#include "arrow/flight/transport/ucx/ucx.h"
#include "arrow/gpu/cuda_api.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"

// TODO: ensure UCX headers are not in public api

namespace arrow {
namespace flight {

class SimpleTestServer : public FlightServerBase {
 public:
  Status GetFlightInfo(const ServerCallContext& context, const FlightDescriptor& request,
                       std::unique_ptr<FlightInfo>* info) override {
    auto examples = ExampleFlightInfo();
    *info = std::unique_ptr<FlightInfo>(new FlightInfo(examples[0]));

    if (request.path.size() > 0 && request.path[0] == "error") {
      return Status::Invalid("test");
    }
    return Status::OK();
  }

  Status DoGet(const ServerCallContext& context, const Ticket& request,
               std::unique_ptr<FlightDataStream>* data_stream) override {
    BatchVector batches;
    RETURN_NOT_OK(ExampleIntBatches(&batches));
    auto batch_reader = std::make_shared<BatchIterator>(batches[0]->schema(), batches);
    *data_stream = std::unique_ptr<FlightDataStream>(new RecordBatchStream(batch_reader));
    return Status::OK();
  }
};

class TestUcx : public ::testing::Test {
 public:
  void SetUp() {
    transport::ucx::InitializeFlightUcx();

    Location location;
    ASSERT_OK(Location::Parse("ucx://0.0.0.0:0", &location));

    ASSERT_OK(MakeServer<SimpleTestServer>(
        location, &server_, &client_,
        [](FlightServerOptions* options) { return Status::OK(); },
        [](FlightClientOptions* options) { return Status::OK(); }));
  }

  void TearDown() { ASSERT_OK(server_->Shutdown()); }

 protected:
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

TEST_F(TestUcx, GetFlightInfo) {
  auto descriptor = FlightDescriptor::Path({"foo", "bar"});
  std::unique_ptr<FlightInfo> info;
  ASSERT_OK(client_->GetFlightInfo(descriptor, &info));
  // Test that we can reuse the connection
  ASSERT_OK(client_->GetFlightInfo(descriptor, &info));
}

TEST_F(TestUcx, DoGet) {
  // TODO: zero-length ticket serializes to zero-length protobuf, trips assertion failure
  // in UCX?
  Ticket ticket{"a"};
  std::unique_ptr<FlightStreamReader> stream;
  ASSERT_OK(client_->DoGet(ticket, &stream));
  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));
  // TODO: if we hit an NYI, we just hang on shutdown?
}

TEST_F(TestUcx, DoGetCuda) {
  // TODO: split this into its own cc file and conditionally include
  ASSERT_OK_AND_ASSIGN(auto manager, cuda::CudaDeviceManager::Instance());
  ASSERT_OK_AND_ASSIGN(auto device, manager->GetDevice(0));

  FlightCallOptions options;
  options.memory_manager = device->default_memory_manager();

  Ticket ticket{"a"};
  std::unique_ptr<FlightStreamReader> stream;
  ASSERT_OK(client_->DoGet(options, ticket, &stream));
  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  for (const auto& column : table->columns()) {
    for (const auto& chunk : column->chunks()) {
      for (const auto& buffer : chunk->data()->buffers) {
        if (!buffer) continue;
        ASSERT_TRUE(buffer->device()->Equals(*device))
            << "Expected buffer on device " << device->ToString()
            << " but was allocated on device " << buffer->device()->ToString();
      }
    }
  }
}

TEST_F(TestUcx, Errors) {
  auto descriptor = FlightDescriptor::Path({"error", "bar"});
  std::unique_ptr<FlightInfo> info;
  ASSERT_RAISES(Invalid, client_->GetFlightInfo(descriptor, &info));
}

}  // namespace flight
}  // namespace arrow
