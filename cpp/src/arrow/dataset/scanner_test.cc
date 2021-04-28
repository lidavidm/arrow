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

#include "arrow/dataset/scanner.h"

#include <memory>
#include <ostream>
#include <sstream>

#include <gmock/gmock.h>

#include "arrow/compute/api.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/dataset/scanner_internal.h"
#include "arrow/dataset/test_util.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/range.h"

using testing::ElementsAre;
using testing::IsEmpty;

namespace arrow {
namespace dataset {

struct TestScannerParams {
  bool use_async;
  bool use_threads;
  int num_child_datasets;
  int num_batches;
  int items_per_batch;

  std::string ToString() const {
    // GTest requires this to be alphanumeric
    std::stringstream ss;
    ss << (use_async ? "Async" : "Sync") << (use_threads ? "Threaded" : "Serial")
       << num_child_datasets << "d" << num_batches << "b" << items_per_batch << "r";
    return ss.str();
  }

  static std::string ToTestNameString(
      const ::testing::TestParamInfo<TestScannerParams>& info) {
    return std::to_string(info.index) + info.param.ToString();
  }

  static std::vector<TestScannerParams> Values() {
    std::vector<TestScannerParams> values;
    for (int sync = 0; sync < 2; sync++) {
      for (int use_threads = 0; use_threads < 2; use_threads++) {
        values.push_back(
            {static_cast<bool>(sync), static_cast<bool>(use_threads), 1, 1, 1024});
        values.push_back(
            {static_cast<bool>(sync), static_cast<bool>(use_threads), 2, 16, 1024});
      }
    }
    return values;
  }
};

std::ostream& operator<<(std::ostream& out, const TestScannerParams& params) {
  out << (params.use_async ? "async-" : "sync-")
      << (params.use_threads ? "threaded-" : "serial-") << params.num_child_datasets
      << "d-" << params.num_batches << "b-" << params.items_per_batch << "i";
  return out;
}

class TestScanner : public DatasetFixtureMixinWithParam<TestScannerParams> {
 protected:
  std::shared_ptr<Scanner> MakeScanner(std::shared_ptr<Dataset> dataset) {
    ScannerBuilder builder(std::move(dataset), options_);
    ARROW_EXPECT_OK(builder.UseThreads(GetParam().use_threads));
    ARROW_EXPECT_OK(builder.UseAsync(GetParam().use_async));
    EXPECT_OK_AND_ASSIGN(auto scanner, builder.Finish());
    return scanner;
  }

  std::shared_ptr<Scanner> MakeScanner(std::shared_ptr<RecordBatch> batch) {
    std::vector<std::shared_ptr<RecordBatch>> batches{
        static_cast<size_t>(GetParam().num_batches), batch};

    DatasetVector children{static_cast<size_t>(GetParam().num_child_datasets),
                           std::make_shared<InMemoryDataset>(batch->schema(), batches)};

    EXPECT_OK_AND_ASSIGN(auto dataset, UnionDataset::Make(batch->schema(), children));
    return MakeScanner(std::move(dataset));
  }

  void AssertScannerEqualsRepetitionsOf(
      std::shared_ptr<Scanner> scanner, std::shared_ptr<RecordBatch> batch,
      const int64_t total_batches = GetParam().num_child_datasets *
                                    GetParam().num_batches) {
    auto expected = ConstantArrayGenerator::Repeat(total_batches, batch);

    // Verifies that the unified BatchReader is equivalent to flattening all the
    // structures of the scanner, i.e. Scanner[Dataset[ScanTask[RecordBatch]]]
    AssertScannerEquals(expected.get(), scanner.get());
  }

  void AssertScanBatchesEqualRepetitionsOf(
      std::shared_ptr<Scanner> scanner, std::shared_ptr<RecordBatch> batch,
      const int64_t total_batches = GetParam().num_child_datasets *
                                    GetParam().num_batches) {
    auto expected = ConstantArrayGenerator::Repeat(total_batches, batch);

    AssertScanBatchesEquals(expected.get(), scanner.get());
  }

  void AssertScanBatchesUnorderedEqualRepetitionsOf(
      std::shared_ptr<Scanner> scanner, std::shared_ptr<RecordBatch> batch,
      const int64_t total_batches = GetParam().num_child_datasets *
                                    GetParam().num_batches) {
    auto expected = ConstantArrayGenerator::Repeat(total_batches, batch);

    AssertScanBatchesUnorderedEquals(expected.get(), scanner.get(), 1);
  }
};

TEST_P(TestScanner, Scan) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  AssertScanBatchesUnorderedEqualRepetitionsOf(MakeScanner(batch), batch);
}

TEST_P(TestScanner, ScanBatches) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  AssertScanBatchesEqualRepetitionsOf(MakeScanner(batch), batch);
}

TEST_P(TestScanner, ScanBatchesUnordered) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  AssertScanBatchesUnorderedEqualRepetitionsOf(MakeScanner(batch), batch);
}

TEST_P(TestScanner, ScanWithCappedBatchSize) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  options_->batch_size = GetParam().items_per_batch / 2;
  auto expected = batch->Slice(GetParam().items_per_batch / 2);
  AssertScanBatchesEqualRepetitionsOf(
      MakeScanner(batch), expected,
      GetParam().num_child_datasets * GetParam().num_batches * 2);
}

TEST_P(TestScanner, FilteredScan) {
  SetSchema({field("f64", float64())});

  double value = 0.5;
  ASSERT_OK_AND_ASSIGN(auto f64,
                       ArrayFromBuilderVisitor(float64(), GetParam().items_per_batch,
                                               GetParam().items_per_batch / 2,
                                               [&](DoubleBuilder* builder) {
                                                 builder->UnsafeAppend(value);
                                                 builder->UnsafeAppend(-value);
                                                 value += 1.0;
                                               }));

  SetFilter(greater(field_ref("f64"), literal(0.0)));

  auto batch = RecordBatch::Make(schema_, f64->length(), {f64});

  value = 0.5;
  ASSERT_OK_AND_ASSIGN(auto f64_filtered,
                       ArrayFromBuilderVisitor(float64(), GetParam().items_per_batch / 2,
                                               [&](DoubleBuilder* builder) {
                                                 builder->UnsafeAppend(value);
                                                 value += 1.0;
                                               }));

  auto filtered_batch =
      RecordBatch::Make(schema_, f64_filtered->length(), {f64_filtered});

  AssertScanBatchesEqualRepetitionsOf(MakeScanner(batch), filtered_batch);
}

TEST_P(TestScanner, ProjectedScan) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  SetProjectedColumns({"i32"});
  auto batch_in = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  auto batch_out = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch,
                                                  schema({field("i32", int32())}));
  AssertScanBatchesUnorderedEqualRepetitionsOf(MakeScanner(batch_in), batch_out);
}

TEST_P(TestScanner, MaterializeMissingColumn) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch_missing_f64 = ConstantArrayGenerator::Zeroes(
      GetParam().items_per_batch, schema({field("i32", int32())}));

  auto fragment_missing_f64 = std::make_shared<InMemoryFragment>(
      RecordBatchVector{
          static_cast<size_t>(GetParam().num_child_datasets * GetParam().num_batches),
          batch_missing_f64},
      equal(field_ref("f64"), literal(2.5)));

  ASSERT_OK_AND_ASSIGN(auto f64,
                       ArrayFromBuilderVisitor(
                           float64(), GetParam().items_per_batch,
                           [&](DoubleBuilder* builder) { builder->UnsafeAppend(2.5); }));
  auto batch_with_f64 =
      RecordBatch::Make(schema_, f64->length(), {batch_missing_f64->column(0), f64});

  FragmentVector fragments{fragment_missing_f64};
  auto dataset = std::make_shared<FragmentDataset>(schema_, fragments);
  auto scanner = MakeScanner(std::move(dataset));
  AssertScanBatchesEqualRepetitionsOf(scanner, batch_with_f64);
}

TEST_P(TestScanner, ToTable) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  std::vector<std::shared_ptr<RecordBatch>> batches{
      static_cast<std::size_t>(GetParam().num_batches * GetParam().num_child_datasets),
      batch};

  ASSERT_OK_AND_ASSIGN(auto expected, Table::FromRecordBatches(batches));

  auto scanner = MakeScanner(batch);
  std::shared_ptr<Table> actual;

  // There is no guarantee on the ordering when using multiple threads, but
  // since the RecordBatch is always the same it will pass.
  ASSERT_OK_AND_ASSIGN(actual, scanner->ToTable());
  AssertTablesEqual(*expected, *actual);
}

TEST_P(TestScanner, ScanWithVisitor) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  auto scanner = MakeScanner(batch);
  ASSERT_OK(scanner->Scan([batch](TaggedRecordBatch scanned_batch) {
    AssertBatchesEqual(*batch, *scanned_batch.record_batch);
    return Status::OK();
  }));
}

TEST_P(TestScanner, TakeIndices) {
  auto batch_size = GetParam().items_per_batch;
  auto num_batches = GetParam().num_batches;
  auto num_datasets = GetParam().num_child_datasets;
  SetSchema({field("i32", int32()), field("f64", float64())});
  ArrayVector arrays(2);
  ArrayFromVector<Int32Type>(internal::Iota<int32_t>(batch_size), &arrays[0]);
  ArrayFromVector<DoubleType>(internal::Iota<double>(static_cast<double>(batch_size)),
                              &arrays[1]);
  auto batch = RecordBatch::Make(schema_, batch_size, arrays);

  auto scanner = MakeScanner(batch);

  std::shared_ptr<Array> indices;
  {
    ArrayFromVector<Int64Type>(internal::Iota(batch_size), &indices);
    ASSERT_OK_AND_ASSIGN(auto taken, scanner->TakeRows(*indices));
    ASSERT_OK_AND_ASSIGN(auto expected, Table::FromRecordBatches({batch}));
    ASSERT_EQ(expected->num_rows(), batch_size);
    AssertTablesEqual(*expected, *taken);
  }
  {
    ArrayFromVector<Int64Type>({7, 5, 3, 1}, &indices);
    ASSERT_OK_AND_ASSIGN(auto taken, scanner->TakeRows(*indices));
    ASSERT_OK_AND_ASSIGN(auto table, scanner->ToTable());
    ASSERT_OK_AND_ASSIGN(auto expected, compute::Take(table, *indices));
    ASSERT_EQ(expected.table()->num_rows(), 4);
    AssertTablesEqual(*expected.table(), *taken);
  }
  if (num_batches > 1) {
    ArrayFromVector<Int64Type>({batch_size + 2, batch_size + 1}, &indices);
    ASSERT_OK_AND_ASSIGN(auto table, scanner->ToTable());
    ASSERT_OK_AND_ASSIGN(auto taken, scanner->TakeRows(*indices));
    ASSERT_OK_AND_ASSIGN(auto expected, compute::Take(table, *indices));
    ASSERT_EQ(expected.table()->num_rows(), 2);
    AssertTablesEqual(*expected.table(), *taken);
  }
  if (num_batches > 1) {
    ArrayFromVector<Int64Type>({1, 3, 5, 7, batch_size + 1, 2 * batch_size + 2},
                               &indices);
    ASSERT_OK_AND_ASSIGN(auto taken, scanner->TakeRows(*indices));
    ASSERT_OK_AND_ASSIGN(auto table, scanner->ToTable());
    ASSERT_OK_AND_ASSIGN(auto expected, compute::Take(table, *indices));
    ASSERT_EQ(expected.table()->num_rows(), 6);
    AssertTablesEqual(*expected.table(), *taken);
  }
  {
    auto base = num_datasets * num_batches * batch_size;
    ArrayFromVector<Int64Type>({base + 1}, &indices);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IndexError,
        ::testing::HasSubstr("Some indices were out of bounds: " +
                             std::to_string(base + 1)),
        scanner->TakeRows(*indices));
  }
  {
    auto base = num_datasets * num_batches * batch_size;
    ArrayFromVector<Int64Type>(
        {1, 2, base + 1, base + 2, base + 3, base + 4, base + 5, base + 6}, &indices);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IndexError,
        ::testing::HasSubstr(
            "Some indices were out of bounds: " + std::to_string(base + 1) + ", " +
            std::to_string(base + 2) + ", " + std::to_string(base + 3) + ", ..."),
        scanner->TakeRows(*indices));
  }
}

TEST_P(TestScanner, CountRows) {
  const auto items_per_batch = GetParam().items_per_batch;
  const auto num_batches = GetParam().num_batches;
  const auto num_datasets = GetParam().num_child_datasets;
  SetSchema({field("i32", int32()), field("f64", float64())});
  ArrayVector arrays(2);
  ArrayFromVector<Int32Type>(
      internal::Iota<int32_t>(static_cast<int32_t>(items_per_batch)), &arrays[0]);
  ArrayFromVector<DoubleType>(
      internal::Iota<double>(static_cast<double>(items_per_batch)), &arrays[1]);
  auto batch = RecordBatch::Make(schema_, items_per_batch, arrays);
  auto scanner = MakeScanner(batch);

  ASSERT_OK_AND_ASSIGN(auto rows, scanner->CountRows());
  ASSERT_EQ(rows, num_datasets * num_batches * items_per_batch);

  ASSERT_OK_AND_ASSIGN(options_->filter,
                       greater_equal(field_ref("i32"), literal(64)).Bind(*schema_));
  ASSERT_OK_AND_ASSIGN(rows, scanner->CountRows());
  ASSERT_EQ(rows, num_datasets * num_batches * (items_per_batch - 64));
}

class CountRowsOnlyFragment : public InMemoryFragment {
 public:
  using InMemoryFragment::InMemoryFragment;

  Result<Future<int64_t>> CountRows(Expression predicate,
                                    std::shared_ptr<ScanOptions>) override {
    if (FieldsInExpression(predicate).size() > 0) return Status::NotImplemented("");
    int64_t sum = 0;
    for (const auto& batch : record_batches_) {
      sum += batch->num_rows();
    }
    return Future<int64_t>::MakeFinished(sum);
  }
  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions>) override {
    return Status::Invalid("Don't scan me!");
  }
  Result<RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<ScanOptions>&) override {
    return Status::Invalid("Don't scan me!");
  }
};

class ScanOnlyFragment : public InMemoryFragment {
 public:
  using InMemoryFragment::InMemoryFragment;

  Result<Future<int64_t>> CountRows(Expression predicate,
                                    std::shared_ptr<ScanOptions>) override {
    return Status::NotImplemented("");
  }
  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions> options) override {
    auto self = shared_from_this();
    ScanTaskVector tasks{
        std::make_shared<InMemoryScanTask>(record_batches_, options, self)};
    return MakeVectorIterator(std::move(tasks));
  }
  Result<RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<ScanOptions>&) override {
    return MakeVectorGenerator(record_batches_);
  }
};

// Ensure the pipeline does not break on an empty batch
TEST_P(TestScanner, CountRowsEmpty) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto empty_batch = ConstantArrayGenerator::Zeroes(0, schema_);
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  RecordBatchVector batches = {empty_batch, batch};
  ScannerBuilder builder(
      std::make_shared<FragmentDataset>(
          schema_, FragmentVector{std::make_shared<ScanOnlyFragment>(batches)}),
      options_);
  ASSERT_OK(builder.UseAsync(GetParam().use_async));
  ASSERT_OK(builder.UseThreads(GetParam().use_threads));
  ASSERT_OK_AND_ASSIGN(auto scanner, builder.Finish());
  ASSERT_OK_AND_EQ(batch->num_rows(), scanner->CountRows());
}

TEST_P(TestScanner, CountRowsWithMetadata) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  RecordBatchVector batches = {batch, batch, batch, batch};
  ScannerBuilder builder(
      std::make_shared<FragmentDataset>(
          schema_, FragmentVector{std::make_shared<CountRowsOnlyFragment>(batches)}),
      options_);
  ASSERT_OK(builder.UseAsync(GetParam().use_async));
  ASSERT_OK(builder.UseThreads(GetParam().use_threads));
  ASSERT_OK_AND_ASSIGN(auto scanner, builder.Finish());
  ASSERT_OK_AND_EQ(4 * batch->num_rows(), scanner->CountRows());

  ASSERT_OK(builder.Filter(equal(field_ref("i32"), literal(5))));
  ASSERT_OK_AND_ASSIGN(scanner, builder.Finish());
  // Scanner should fall back on reading data and hit the error
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("Don't scan me!"),
                                  scanner->CountRows());
}

class FailingFragment : public InMemoryFragment {
 public:
  using InMemoryFragment::InMemoryFragment;
  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions> options) override {
    int index = 0;
    auto self = shared_from_this();
    return MakeFunctionIterator([=]() mutable -> Result<std::shared_ptr<ScanTask>> {
      if (index > 16) {
        return Status::Invalid("Oh no, we failed!");
      }
      RecordBatchVector batches = {record_batches_[index++ % record_batches_.size()]};
      return std::make_shared<InMemoryScanTask>(batches, options, self);
    });
  }

  Result<RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<ScanOptions>& options) override {
    struct {
      Future<std::shared_ptr<RecordBatch>> operator()() {
        if (index > 16) {
          return Status::Invalid("Oh no, we failed!");
        }
        auto batch = batches[index++ % batches.size()];
        return Future<std::shared_ptr<RecordBatch>>::MakeFinished(batch);
      }
      RecordBatchVector batches;
      int index = 0;
    } Generator;
    Generator.batches = record_batches_;
    return Generator;
  }
};

class FailingExecuteScanTask : public InMemoryScanTask {
 public:
  using InMemoryScanTask::InMemoryScanTask;

  Result<RecordBatchIterator> Execute() override {
    return Status::Invalid("Oh no, we failed!");
  }
};

class FailingIterationScanTask : public InMemoryScanTask {
 public:
  using InMemoryScanTask::InMemoryScanTask;

  Result<RecordBatchIterator> Execute() override {
    int index = 0;
    auto batches = record_batches_;
    return MakeFunctionIterator(
        [index, batches]() mutable -> Result<std::shared_ptr<RecordBatch>> {
          if (index < 1) {
            return batches[index++];
          }
          return Status::Invalid("Oh no, we failed!");
        });
  }
};

template <typename T>
class FailingScanTaskFragment : public InMemoryFragment {
 public:
  using InMemoryFragment::InMemoryFragment;
  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions> options) override {
    auto self = shared_from_this();
    ScanTaskVector scan_tasks{std::make_shared<T>(record_batches_, options, self)};
    return MakeVectorIterator(std::move(scan_tasks));
  }

  // Unlike the sync case, there's only two places to fail - during
  // iteration (covered by FailingFragment) or at the initial scan
  // (covered here)
  Result<RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<ScanOptions>& options) override {
    return Status::Invalid("Oh no, we failed!");
  }
};

template <typename It, typename GetBatch>
bool CheckIteratorRaises(const RecordBatch& batch, It batch_it, GetBatch get_batch) {
  while (true) {
    auto maybe_batch = batch_it.Next();
    if (maybe_batch.ok()) {
      EXPECT_OK_AND_ASSIGN(auto scanned_batch, maybe_batch);
      if (IsIterationEnd(scanned_batch)) break;
      AssertBatchesEqual(batch, *get_batch(scanned_batch));
    } else {
      EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("Oh no, we failed!"),
                                      maybe_batch);
      return true;
    }
  }
  return false;
}

TEST_P(TestScanner, ScanBatchesFailure) {
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(GetParam().items_per_batch, schema_);
  RecordBatchVector batches = {batch, batch, batch, batch};

  auto check_scanner = [](const RecordBatch& batch, Scanner* scanner) {
    auto maybe_batch_it = scanner->ScanBatchesUnordered();
    if (!maybe_batch_it.ok()) {
      // SyncScanner can fail here as it eagerly consumes the first value
      EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("Oh no, we failed!"),
                                      std::move(maybe_batch_it));
    } else {
      ASSERT_OK_AND_ASSIGN(auto batch_it, std::move(maybe_batch_it));
      EXPECT_TRUE(CheckIteratorRaises(
          batch, std::move(batch_it),
          [](const EnumeratedRecordBatch& batch) { return batch.record_batch.value; }))
          << "ScanBatchesUnordered() did not raise an error";
    }
    ASSERT_OK_AND_ASSIGN(auto tagged_batch_it, scanner->ScanBatches());
    EXPECT_TRUE(CheckIteratorRaises(
        batch, std::move(tagged_batch_it),
        [](const TaggedRecordBatch& batch) { return batch.record_batch; }))
        << "ScanBatches() did not raise an error";
  };

  // Case 1: failure when getting next scan task
  {
    FragmentVector fragments{std::make_shared<FailingFragment>(batches),
                             std::make_shared<InMemoryFragment>(batches)};
    auto dataset = std::make_shared<FragmentDataset>(schema_, fragments);
    auto scanner = MakeScanner(std::move(dataset));
    check_scanner(*batch, scanner.get());
  }

  // Case 2: failure when calling ScanTask::Execute
  {
    FragmentVector fragments{
        std::make_shared<FailingScanTaskFragment<FailingExecuteScanTask>>(batches),
        std::make_shared<InMemoryFragment>(batches)};
    auto dataset = std::make_shared<FragmentDataset>(schema_, fragments);
    auto scanner = MakeScanner(std::move(dataset));
    check_scanner(*batch, scanner.get());
  }

  // Case 3: failure when calling RecordBatchIterator::Next
  {
    FragmentVector fragments{
        std::make_shared<FailingScanTaskFragment<FailingIterationScanTask>>(batches),
        std::make_shared<InMemoryFragment>(batches)};
    auto dataset = std::make_shared<FragmentDataset>(schema_, fragments);
    auto scanner = MakeScanner(std::move(dataset));
    check_scanner(*batch, scanner.get());
  }
}

TEST_P(TestScanner, Head) {
  auto batch_size = GetParam().items_per_batch;
  auto num_batches = GetParam().num_batches;
  auto num_datasets = GetParam().num_child_datasets;
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(batch_size, schema_);

  auto scanner = MakeScanner(batch);
  std::shared_ptr<Table> expected, actual;

  ASSERT_OK_AND_ASSIGN(expected, Table::FromRecordBatches(schema_, {}));
  ASSERT_OK_AND_ASSIGN(actual, scanner->Head(0));
  AssertTablesEqual(*expected, *actual);

  ASSERT_OK_AND_ASSIGN(expected, Table::FromRecordBatches(schema_, {batch}));
  ASSERT_OK_AND_ASSIGN(actual, scanner->Head(batch_size));
  AssertTablesEqual(*expected, *actual);

  ASSERT_OK_AND_ASSIGN(expected, Table::FromRecordBatches(schema_, {batch->Slice(0, 1)}));
  ASSERT_OK_AND_ASSIGN(actual, scanner->Head(1));
  AssertTablesEqual(*expected, *actual);

  if (num_batches > 1) {
    ASSERT_OK_AND_ASSIGN(expected,
                         Table::FromRecordBatches(schema_, {batch, batch->Slice(0, 1)}));
    ASSERT_OK_AND_ASSIGN(actual, scanner->Head(batch_size + 1));
    AssertTablesEqual(*expected, *actual);
  }

  ASSERT_OK_AND_ASSIGN(expected, scanner->ToTable());
  ASSERT_OK_AND_ASSIGN(actual, scanner->Head(batch_size * num_batches * num_datasets));
  AssertTablesEqual(*expected, *actual);

  ASSERT_OK_AND_ASSIGN(expected, scanner->ToTable());
  ASSERT_OK_AND_ASSIGN(actual,
                       scanner->Head(batch_size * num_batches * num_datasets + 100));
  AssertTablesEqual(*expected, *actual);
}

INSTANTIATE_TEST_SUITE_P(TestScannerThreading, TestScanner,
                         ::testing::ValuesIn(TestScannerParams::Values()),
                         [](const ::testing::TestParamInfo<TestScannerParams>& info) {
                           return std::to_string(info.index) + info.param.ToString();
                         });

class TestScannerBuilder : public ::testing::Test {
  void SetUp() override {
    DatasetVector sources;

    schema_ = schema({
        field("b", boolean()),
        field("i8", int8()),
        field("i16", int16()),
        field("i32", int32()),
        field("i64", int64()),
    });

    ASSERT_OK_AND_ASSIGN(dataset_, UnionDataset::Make(schema_, sources));
  }

 protected:
  std::shared_ptr<ScanOptions> options_ = std::make_shared<ScanOptions>();
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<Dataset> dataset_;
};

TEST_F(TestScannerBuilder, TestProject) {
  ScannerBuilder builder(dataset_, options_);

  // It is valid to request no columns, e.g. `SELECT 1 FROM t WHERE t.a > 0`.
  // still needs to touch the `a` column.
  ASSERT_OK(builder.Project({}));
  ASSERT_OK(builder.Project({"i64", "b", "i8"}));
  ASSERT_OK(builder.Project({"i16", "i16"}));
  ASSERT_OK(builder.Project(
      {field_ref("i16"), call("multiply", {field_ref("i16"), literal(2)})},
      {"i16 renamed", "i16 * 2"}));

  ASSERT_RAISES(Invalid, builder.Project({"not_found_column"}));
  ASSERT_RAISES(Invalid, builder.Project({"i8", "not_found_column"}));
  ASSERT_RAISES(Invalid,
                builder.Project({field_ref("not_found_column"),
                                 call("multiply", {field_ref("i16"), literal(2)})},
                                {"i16 renamed", "i16 * 2"}));

  ASSERT_RAISES(NotImplemented, builder.Project({field_ref(FieldRef("nested", "column"))},
                                                {"nested column"}));

  // provided more field names than column exprs or vice versa
  ASSERT_RAISES(Invalid, builder.Project({}, {"i16 renamed", "i16 * 2"}));
  ASSERT_RAISES(Invalid, builder.Project({literal(2), field_ref("a")}, {"a"}));
}

TEST_F(TestScannerBuilder, TestFilter) {
  ScannerBuilder builder(dataset_, options_);

  ASSERT_OK(builder.Filter(literal(true)));
  ASSERT_OK(builder.Filter(equal(field_ref("i64"), literal<int64_t>(10))));
  ASSERT_OK(builder.Filter(or_(equal(field_ref("i64"), literal<int64_t>(10)),
                               equal(field_ref("b"), literal(true)))));

  ASSERT_OK(builder.Filter(equal(field_ref("i64"), literal<double>(10))));

  ASSERT_RAISES(Invalid, builder.Filter(equal(field_ref("not_a_column"), literal(true))));

  ASSERT_RAISES(
      NotImplemented,
      builder.Filter(equal(field_ref(FieldRef("nested", "column")), literal(true))));

  ASSERT_RAISES(Invalid,
                builder.Filter(or_(equal(field_ref("i64"), literal<int64_t>(10)),
                                   equal(field_ref("not_a_column"), literal(true)))));
}

TEST(ScanOptions, TestMaterializedFields) {
  auto i32 = field("i32", int32());
  auto i64 = field("i64", int64());
  auto opts = std::make_shared<ScanOptions>();

  // empty dataset, project nothing = nothing materialized
  opts->dataset_schema = schema({});
  ASSERT_OK(SetProjection(opts.get(), {}, {}));
  EXPECT_THAT(opts->MaterializedFields(), IsEmpty());

  // non-empty dataset, project nothing = nothing materialized
  opts->dataset_schema = schema({i32, i64});
  EXPECT_THAT(opts->MaterializedFields(), IsEmpty());

  // project nothing, filter on i32 = materialize i32
  opts->filter = equal(field_ref("i32"), literal(10));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre("i32"));

  // project i32 & i64, filter nothing = materialize i32 & i64
  opts->filter = literal(true);
  ASSERT_OK(SetProjection(opts.get(), {"i32", "i64"}));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre("i32", "i64"));

  // project i32 + i64, filter nothing = materialize i32 & i64
  opts->filter = literal(true);
  ASSERT_OK(SetProjection(opts.get(), {call("add", {field_ref("i32"), field_ref("i64")})},
                          {"i32 + i64"}));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre("i32", "i64"));

  // project i32, filter nothing = materialize i32
  ASSERT_OK(SetProjection(opts.get(), {"i32"}));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre("i32"));

  // project i32, filter on i32 = materialize i32 (reported twice)
  opts->filter = equal(field_ref("i32"), literal(10));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre("i32", "i32"));

  // project i32, filter on i32 & i64 = materialize i64, i32 (reported twice)
  opts->filter = less(field_ref("i32"), field_ref("i64"));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre("i32", "i64", "i32"));

  // project i32, filter on i64 = materialize i32 & i64
  opts->filter = equal(field_ref("i64"), literal(10));
  EXPECT_THAT(opts->MaterializedFields(), ElementsAre("i64", "i32"));
}

}  // namespace dataset
}  // namespace arrow
