//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  // 在初始化阶段构建好hash_table
  if (left_child_) {
    left_child_->Init();
  }
  if (right_child_) {
    right_child_->Init();
  }
  auto left_plan = plan_->GetLeftPlan();
  Tuple left_tuple;
  RID left_rid;
  while (left_child_->Next(&left_tuple, &left_rid)) {
    HashJoinKey left_join_key;
    left_join_key.val_ = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, left_plan->OutputSchema());
    if (ht_.count(left_join_key) != 0U) {
      ht_[left_join_key].push_back(left_tuple);
    } else {
      std::vector<Tuple> tmp = {left_tuple};
      ht_[left_join_key] = tmp;
    }
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  auto right_plan = plan_->GetRightPlan();
  auto left_plan = plan_->GetLeftPlan();
  Tuple right_tuple;
  RID right_rid;
  // right_child_->Init();
  while (right_child_->Next(&right_tuple, &right_rid)) {
    HashJoinKey right_join_key;
    right_join_key.val_ = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, right_plan->OutputSchema());
    if (ht_.count(right_join_key) != 0U) {
      if (!ht_.empty()) {
        Tuple left_tuple = ht_[right_join_key].back();
        // ht_[right_join_key].pop_back();
        auto left_out_put_schema = left_plan->OutputSchema();
        auto right_out_put_schema = right_plan->OutputSchema();
        std::vector<Value> vals;
        std::vector<Column> columns = plan_->OutputSchema()->GetColumns();
        vals.resize(columns.size());
        size_t i = 0;
        for (const auto &col : columns) {
          vals[i++] = col.GetExpr()->EvaluateJoin(&left_tuple, left_out_put_schema, &right_tuple, right_out_put_schema);
        }
        left_tuple = Tuple(vals, plan_->OutputSchema());
        *tuple = left_tuple;
        *rid = left_tuple.GetRid();
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub
