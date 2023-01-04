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

#include <atomic>
#include <charconv>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/flight/client.h"
#include "arrow/flight/server.h"
#include "arrow/flight/types.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"

// A test/demo of new Flight proposals.

namespace arrow::flight {

/// Static data
struct KnownDatasets {
  struct KnownDataset {
    std::vector<std::string> path;
    std::vector<std::shared_ptr<arrow::Table>> data;
  };

  std::vector<KnownDataset> datasets = {
      KnownDataset{/*path=*/{"ints"},
                   /*data=*/
                   {
                       TableFromJSON(schema({field("ints", int64())}),
                                     {R"([[5], [6], [7], [8]])"}),
                       TableFromJSON(schema({field("ints", int64())}),
                                     {R"([[1], [2], [3], [4]])"}),
                   }},
      KnownDataset{/*path=*/{"ordered_ints"},
                   /*data=*/
                   {
                       TableFromJSON(schema({field("ints", int64())}),
                                     {R"([[1], [2], [3], [4]])"}),
                       TableFromJSON(schema({field("ints", int64())}),
                                     {R"([[5], [6], [7], [8]])"}),
                   }},
  };

  arrow::Result<const KnownDataset*> FindDataset(
      const std::vector<std::string>& path) const {
    for (const auto& dataset : KnownDatasets::Instance().datasets) {
      if (dataset.path == path) {
        return &dataset;
      }
    }
    return Status::KeyError("Dataset not found");
  }

  static const KnownDatasets& Instance() {
    static KnownDatasets datasets;
    return datasets;
  }
};

/// A worker that reads part of a dataset.
class DatasetWorker : public FlightServerBase {
 public:
  explicit DatasetWorker(size_t index) : index_(index) {}
  Status GetFlightInfo(const ServerCallContext& context, const FlightDescriptor& request,
                       std::unique_ptr<FlightInfo>* info) override {
    if (!request.accept_partial) {
      return Status::Invalid("Blocking requests");
    }

    if (request.type == FlightDescriptor::DescriptorType::CMD) {
      // Check progress
      const int64_t query_id =
          std::strtoll(request.cmd.c_str(), /*str_end=*/nullptr, /*base=*/10);
      auto it = running_queries_.find(query_id);
      if (it == running_queries_.end()) {
        return Status::KeyError(request.cmd);
      }
      QueryState* query_state = &it->second;
      std::optional<FlightInfo::RetryInfo> retry_info = std::nullopt;
      if (--query_state->progress == 0) {
        retry_info = {
            request,
            /*progress=*/(3.0 - query_state->progress) / 3.0,
        };
      }
      ARROW_ASSIGN_OR_RAISE(
          FlightInfo result,
          FlightInfo::Make(*query_state->requested_dataset->data[0]->schema(), request,
                           /*endpoints=*/{},
                           /*total_records=*/-1,
                           /*total_bytes=*/-1,
                           /*endpoints_ordered=*/true,
                           /*retry_info=*/
                           FlightInfo::RetryInfo{
                               request,
                               /*progress=*/0.0,
                           }));
      *info = std::make_unique<FlightInfo>(std::move(result));
      if (query_state->progress == 0) {
        running_queries_.erase(it);
      }
      return Status::OK();
    }

    QueryState query_state;
    ARROW_ASSIGN_OR_RAISE(query_state.requested_dataset,
                          KnownDatasets::Instance().FindDataset(request.path));

    // Return empty result
    const int64_t query_id = query_id_generator_.fetch_add(1);
    FlightDescriptor retry_descriptor =
        FlightDescriptor::Command(std::to_string(query_id));
    ARROW_ASSIGN_OR_RAISE(
        FlightInfo result,
        FlightInfo::Make(*query_state.requested_dataset->data[0]->schema(), request,
                         /*endpoints=*/{},
                         /*total_records=*/-1,
                         /*total_bytes=*/-1,
                         /*endpoints_ordered=*/true,
                         /*retry_info=*/
                         FlightInfo::RetryInfo{
                             std::move(retry_descriptor),
                             /*progress=*/0.0,
                         }));
    *info = std::make_unique<FlightInfo>(std::move(result));

    // Save query state
    running_queries_.insert({query_id, std::move(query_state)});
    return Status::OK();
  }

  Status DoGet(const ServerCallContext& context, const Ticket& request,
               std::unique_ptr<FlightDataStream>* stream) override {
    return Status::NotImplemented("");
  }

 private:
  struct QueryState {
    const KnownDatasets::KnownDataset* requested_dataset = nullptr;
    // To emulate 'work', just don't return a complete response until
    // a certain amount of requests have been made
    int32_t progress = 3;
  };
  size_t index_;
  std::atomic<int64_t> query_id_generator_;
  std::unordered_map<int64_t, QueryState> running_queries_;
};

/// A server that takes requests for datasets and distributes the load
/// across workers.
class DatasetCoordinator : public FlightServerBase {
 public:
  explicit DatasetCoordinator(std::vector<Location> locations)
      : locations_(std::move(locations)) {}

  Status Connect() {
    clients_.reserve(locations_.size());
    for (const auto& location : locations_) {
      ARROW_ASSIGN_OR_RAISE(auto client, FlightClient::Connect(location));
      clients_.push_back(std::move(client));
    }
    return Status::OK();
  }

  Status GetFlightInfo(const ServerCallContext& context, const FlightDescriptor& request,
                       std::unique_ptr<FlightInfo>* info) override {
    if (request.type == FlightDescriptor::DescriptorType::CMD) {
      return GetFlightInfoCheckProgress(context, request, info);
    }
    return GetFlightInfoStartRequest(context, request, info);
  }

  Status GetFlightInfoStartRequest(const ServerCallContext& context,
                                   const FlightDescriptor& request,
                                   std::unique_ptr<FlightInfo>* info) {
    ARROW_ASSIGN_OR_RAISE(const KnownDatasets::KnownDataset* requested_dataset,
                          KnownDatasets::Instance().FindDataset(request.path));

    // Start query on all workers
    QueryState query_state;
    FlightDescriptor worker_descriptor = request;
    worker_descriptor.accept_partial = true;
    for (auto& client : clients_) {
      // XXX: in a real system we'd want to cancel if anything failed here
      ARROW_ASSIGN_OR_RAISE(auto info, client->GetFlightInfo(worker_descriptor));
      query_state.worker_progress.push_back(std::move(info));
    }

    if (!request.accept_partial) {
      // Wait for workers to finish
      return Status::NotImplemented("");
    }

    // Return empty result
    const int64_t query_id = query_id_generator_.fetch_add(1);
    FlightDescriptor retry_descriptor =
        FlightDescriptor::Command(std::to_string(query_id));
    ARROW_ASSIGN_OR_RAISE(FlightInfo result,
                          FlightInfo::Make(*requested_dataset->data[0]->schema(), request,
                                           /*endpoints=*/{},
                                           /*total_records=*/-1,
                                           /*total_bytes=*/-1,
                                           /*endpoints_ordered=*/true,
                                           /*retry_info=*/
                                           FlightInfo::RetryInfo{
                                               std::move(retry_descriptor),
                                               /*progress=*/0.0,
                                           }));
    *info = std::make_unique<FlightInfo>(std::move(result));

    // Save query state
    running_queries_.insert({query_id, std::move(query_state)});
    return Status::OK();
  }

  Status GetFlightInfoCheckProgress(const ServerCallContext& context,
                                    const FlightDescriptor& request,
                                    std::unique_ptr<FlightInfo>* info) {
    return Status::NotImplemented("");
  }

 private:
  struct QueryState {
    std::vector<std::unique_ptr<FlightInfo>> worker_progress;
  };

  std::vector<Location> locations_;
  std::vector<std::unique_ptr<FlightClient>> clients_;
  std::atomic<int64_t> query_id_generator_;
  std::unordered_map<int64_t, QueryState> running_queries_;
};

class DatasetTest : public ::testing::Test {
 public:
  void SetUp() override {
    ASSERT_OK_AND_ASSIGN(auto location, Location::ForGrpcTcp("localhost", 0));
    FlightServerOptions options(location);

    servers_.resize(3);
    servers_[0] = std::make_unique<DatasetWorker>(/*index=*/0);
    ASSERT_OK(servers_[0]->Init(options));
    servers_[1] = std::make_unique<DatasetWorker>(/*index=*/1);
    ASSERT_OK(servers_[1]->Init(options));
    servers_[2] = std::make_unique<DatasetCoordinator>(std::vector<Location>{
        servers_[0]->location(),
        servers_[1]->location(),
    });
    ASSERT_OK(static_cast<DatasetCoordinator*>(servers_[2].get())->Connect());
    ASSERT_OK(servers_[2]->Init(options));

    ASSERT_OK_AND_ASSIGN(client_, FlightClient::Connect(servers_[2]->location()));
  }

  void TearDown() override {
    ASSERT_OK(client_->Close());
    ASSERT_OK(servers_[2]->Shutdown());
    ASSERT_OK(servers_[1]->Shutdown());
    ASSERT_OK(servers_[0]->Shutdown());
  }

 protected:
  std::vector<std::unique_ptr<FlightServerBase>> servers_;
  std::unique_ptr<FlightClient> client_;
};

TEST_F(DatasetTest, NonBlockingRead) {
  FlightDescriptor descriptor = FlightDescriptor::Path({"ints"});
  descriptor.accept_partial = true;

  ASSERT_OK_AND_ASSIGN(auto info, client_->GetFlightInfo(descriptor));
  ASSERT_TRUE(info->retry_info().has_value());
  ASSERT_FALSE(info->endpoints_ordered());

  while (true) {
    ASSERT_OK_AND_ASSIGN(info,
                         client_->GetFlightInfo(info->retry_info()->retry_descriptor));
    if (!info->retry_info().has_value()) break;
  }

  // TODO: fetch data
}

TEST_F(DatasetTest, OrderedRead) {
  FlightDescriptor descriptor = FlightDescriptor::Path({"ordered_ints"});
  ASSERT_FALSE(descriptor.accept_partial);

  ASSERT_OK_AND_ASSIGN(auto info, client_->GetFlightInfo(descriptor));
  ASSERT_FALSE(info->retry_info().has_value());
  ASSERT_TRUE(info->endpoints_ordered());

  // TODO: fetch data
}

TEST_F(DatasetTest, RetriedRead) {
  FlightDescriptor descriptor = FlightDescriptor::Path({"ints"});

  ASSERT_OK_AND_ASSIGN(auto info, client_->GetFlightInfo(descriptor));
  ASSERT_FALSE(info->retry_info().has_value());
  ASSERT_FALSE(info->endpoints_ordered());

  // TODO: check expiration time
}

}  // namespace arrow::flight
