//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  if (child_executor_) {
    child_executor_->Init();
  }
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  auto schema = plan_->OutputSchema();
  while (child_executor_->Next(tuple, rid)) {
    std::vector<Column> columns = schema->GetColumns();
    std::vector<Value> vals;
    vals.resize(columns.size());
    for (size_t i = 0; i < vals.size(); ++i) {
      vals[i] = tuple->GetValue(schema, schema->GetColIdx(columns[i].GetName()));
    }
    DistinctKey tuple_dist_key;
    tuple_dist_key.vals_ = vals;
    if (ht_.count(tuple_dist_key) == 0U) {
      ht_.insert(tuple_dist_key);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
