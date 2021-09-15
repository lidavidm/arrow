// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/compute/exec/expression.h>
#include <arrow/compute/exec/options.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/future.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace cp = ::arrow::compute;
namespace ds = arrow::dataset;
namespace fs = arrow::fs;

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

// TODO: compare with a regular scanner and sync scanner
arrow::Status Main() {
  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

  auto filesystem = std::make_shared<fs::LocalFileSystem>();
  auto format = std::make_shared<ds::ParquetFileFormat>();

  fs::FileSelector s;
  s.base_dir = "/home/lidavidm/Documents/taxi";
  s.recursive = true;
  ds::FileSystemFactoryOptions options;
  options.partitioning = ds::DirectoryPartitioning::MakeFactory({"year", "month"});
  ds::InspectOptions inspect_options;
  inspect_options.fragments = 10;
  ds::FinishOptions finish_options;
  ARROW_ASSIGN_OR_RAISE(
      auto factory, ds::FileSystemDatasetFactory::Make(filesystem, s, format, options));
  ARROW_ASSIGN_OR_RAISE(auto schema, factory->Inspect(inspect_options));
  finish_options.schema = schema;
  ARROW_ASSIGN_OR_RAISE(auto dataset, factory->Finish(finish_options));

  auto filter = cp::and_(cp::less_equal(cp::field_ref("year"), cp::literal(2011)),
                         cp::equal(cp::field_ref("vendor_id"), cp::literal("VTS")));
  std::vector<std::string> names = {};
  ARROW_ASSIGN_OR_RAISE(auto projection, cp::call("make_struct",
                                                  {
                                                      cp::field_ref("passenger_count"),
                                                      cp::field_ref("tip_amount"),
                                                  },
                                                  cp::MakeStructOptions{names})
                                             .Bind(*dataset->schema()));
  auto scan_options = std::make_shared<ds::ScanOptions>();
  scan_options->filter = filter;
  scan_options->projection = projection;
  scan_options->use_async = true;
  scan_options->use_threads = true;

  // Taxi dataset
  cp::ExecContext ctx(arrow::default_memory_pool(), arrow::internal::GetCpuThreadPool());
  ARROW_ASSIGN_OR_RAISE(auto plan, cp::ExecPlan::Make(&ctx));
  ARROW_RETURN_NOT_OK(
      cp::Declaration::Sequence(
          {
              {"scan", ds::ScanNodeOptions{dataset, scan_options}},
              {"filter", cp::FilterNodeOptions{filter}},
              {"project",
               cp::ProjectNodeOptions{
                   {
                       cp::field_ref("passenger_count"),
                       cp::call("divide", {cp::field_ref("tip_amount"),
                                           cp::field_ref("passenger_count")}),
                   },
                   {"passenger_count", "tip_per_passenger"}}},
              {"aggregate",
               cp::AggregateNodeOptions{
                   {{"mean", nullptr}},
                   {"tip_per_passenger"},
                   {"mean_tip_per_passenger"},
               }},
              {"sink", cp::SinkNodeOptions{&sink_gen}},
          })
          .AddToPlan(plan.get())
          .status());

  ARROW_RETURN_NOT_OK(plan->Validate());
  ARROW_RETURN_NOT_OK(plan->StartProducing());

  auto collected = arrow::CollectAsyncGenerator(sink_gen);

  return arrow::AllComplete({plan->finished(), arrow::Future<>(collected)})
      .Then([collected, plan]() -> arrow::Status {
        ARROW_ASSIGN_OR_RAISE(auto batches, collected.result());
        std::cout << "Got " << batches.size() << " batches" << std::endl;
        return arrow::Status::OK();
      })
      .status();
}

int main(int argc, char** argv) {
  ds::internal::Initialize();

  auto start = std::chrono::steady_clock::now();
  ABORT_ON_FAILURE(Main());
  auto end = std::chrono::steady_clock::now();
  std::chrono::duration<double> duration = end - start;

  std::cout << "Took " << duration.count() << "s" << std::endl;

  return EXIT_SUCCESS;
}
