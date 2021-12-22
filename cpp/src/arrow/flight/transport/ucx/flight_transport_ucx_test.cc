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

TEST_F(TestUcx, Basics) {
  auto descriptor = FlightDescriptor::Path({"foo", "bar"});
  std::unique_ptr<FlightInfo> info;
  ASSERT_OK(client_->GetFlightInfo(descriptor, &info));
}

}  // namespace flight
}  // namespace arrow
