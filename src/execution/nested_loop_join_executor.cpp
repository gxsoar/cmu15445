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
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {
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
    std::vector<std::pair<Tuple, RID>> left_values;
    std::vector<std::pair<Tuple, RID>> right_values;
    while(left_executor_->Next(tuple, rid)) {
        left_values.push_back(std::make_pair(*tuple, *rid));
    }
    while(right_executor_->Next(tuple, rid)) {
        right_values.push_back(std::make_pair(*tuple, *rid));
    }
    for (auto &left_val : left_values) {
        Tuple left_tuple = left_val.first;
        RID left_rid = left_val.second;
        for (auto &right_val : right_values) {
            Tuple right_tuple = right_val.first;
            RID right_rid = right_val.second;
            if (predicate == nullptr || 
                predicate->EvaluateJoin(&left_tuple, left_out_put_schema, &right_tuple, right_out_put_schema).GetAs<bool>()) {
                std::vector<Value> values;
                std::vector<Column> left_cols = left_out_put_schema->GetColumns();
                std::vector<Column> right_cols = right_out_put_schema->GetColumns();
                for (const auto &l_col : left_cols) {
                    values.push_back(left_tuple.GetValue(left_out_put_schema, left_out_put_schema->GetColIdx(l_col.GetName())));
                }
                for (const auto &r_col : right_cols) {
                    values.push_back(right_tuple.GetValue(right_out_put_schema, right_out_put_schema->GetColIdx(r_col.GetName())));
                }
                *tuple = Tuple(values, plan_out_put_schema);
                
            }

        }
    }
    return false; 
}

}  // namespace bustub
