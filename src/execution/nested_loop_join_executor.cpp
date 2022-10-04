//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  left_plan_ = plan_->GetLeftPlan();
  right_plan_ = plan_->GetRightPlan();
}

void NestedLoopJoinExecutor::Init() {
  if (left_executor_) {
    // left_table_info_ = left_executor_->GetExecutorContext()->GetCatalog()->GetTable(left_plan_->)
    left_executor_->Init();
  }
  if (right_executor_) {
    right_executor_->Init();
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  const Schema *plan_out_put_schema = plan_->OutputSchema();
  const Schema *left_out_put_schema = left_plan_->OutputSchema();
  const Schema *right_out_put_schema = right_plan_->OutputSchema();
  auto predicate = plan_->Predicate();
  Tuple left_tuple;
  RID left_rid;
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    Tuple right_tuple;
    RID right_rid;
    // right_executor_->Init();
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      if (predicate == nullptr ||
          predicate->EvaluateJoin(&left_tuple, left_out_put_schema, &right_tuple, right_out_put_schema).GetAs<bool>()) {
        std::vector<Value> vals;
        std::vector<Column> columns = plan_out_put_schema->GetColumns();
        vals.resize(columns.size());
        size_t i = 0;
        for (const auto &col : columns) {
          vals[i++] = col.GetExpr()->EvaluateJoin(&left_tuple, left_out_put_schema, &right_tuple, right_out_put_schema);
        }
        left_tuple = Tuple(vals, plan_out_put_schema);
      }
      *tuple = left_tuple;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
