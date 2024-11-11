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

// Performance server for benchmarking purposes

#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>

#include <gflags/gflags.h>

#include "arrow/array.h"
#include "arrow/io/test_common.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type_fwd.h"
#include "arrow/util/config.h"
#include "arrow/util/logging.h"

#include "arrow/flight/api.h"
#include "arrow/flight/test_util.h"

DEFINE_int32(port, 31337, "Server port to listen on");

arrow::Status GetPerfBatches(const std::shared_ptr<arrow::Schema>& schema,
                             arrow::RecordBatchVector& batches) {
  constexpr int32_t total_length = 100'000'000;
  constexpr int32_t ncolumns = 4;
  constexpr int32_t batch_size = 4096;
  int32_t rows = 0;

  while (rows < total_length) {
    std::shared_ptr<arrow::ResizableBuffer> buffer;
    arrow::ArrayVector arrays;

    const int32_t nrows = std::min<int32_t>(batch_size, total_length - rows);
    rows += nrows;

    for (int i = 0; i < ncolumns; ++i) {
      RETURN_NOT_OK(arrow::MakeRandomByteBuffer(nrows * sizeof(int64_t),
                                                arrow::default_memory_pool(), &buffer,
                                                static_cast<int32_t>(i) /* seed */));
      arrays.push_back(std::make_shared<arrow::Int64Array>(nrows, buffer));
      RETURN_NOT_OK(arrays.back()->Validate());
    }
    batches.push_back(arrow::RecordBatch::Make(schema, nrows, arrays));
  }
  return arrow::Status::OK();
}

class FlightPerfServer : public arrow::flight::FlightServerBase {
 public:
  FlightPerfServer() {
    perf_schema_ = arrow::schema({
        arrow::field("a", arrow::int64()),
        arrow::field("b", arrow::int64()),
        arrow::field("c", arrow::int64()),
        arrow::field("d", arrow::int64()),
    });
    ARROW_CHECK_OK(GetPerfBatches(perf_schema_, batches_));
  }

  arrow::Status DoGet(
      const arrow::flight::ServerCallContext&, const arrow::flight::Ticket&,
      std::unique_ptr<arrow::flight::FlightDataStream>* data_stream) override {
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::RecordBatchReader::Make(batches_));
    *data_stream = std::make_unique<arrow::flight::RecordBatchStream>(std::move(reader));
    return arrow::Status::OK();
  }

 private:
  std::shared_ptr<arrow::Schema> perf_schema_;
  arrow::RecordBatchVector batches_;
};

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto server = std::make_unique<FlightPerfServer>();
  arrow::flight::Location bind_location;
  ARROW_CHECK_OK(
      arrow::flight::Location::ForGrpcTcp("0.0.0.0", FLAGS_port).Value(&bind_location));
  arrow::flight::FlightServerOptions options(bind_location);
  ARROW_CHECK_OK(server->Init(options));
  ARROW_CHECK_OK(server->SetShutdownOnSignals({SIGTERM}));
  std::cout << "Server location: " << bind_location.ToString() << std::endl;
  ARROW_CHECK_OK(server->Serve());
  return EXIT_SUCCESS;
}
