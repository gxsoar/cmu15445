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
    std::vector<Value> values;
    Tuple left_tuple;
    RID left_rid;
    while(left_executor_->Next(&left_tuple, &left_rid)) {
        if (!left_tuple.IsAllocated()) {
            break;
        }
        Tuple right_tuple;
        RID right_rid;
        bool flag = false;
        while(right_executor_->Next(&right_tuple, &right_rid)) {
            if (!right_tuple.IsAllocated()) {
                break;
            }
            if (predicate == nullptr ||
             predicate->EvaluateJoin(&left_tuple, left_out_put_schema, &right_tuple, right_out_put_schema).GetAs<bool>()) {
                std::vector<Column> left_column = left_out_put_schema->GetColumns();
                std::vector<Column> right_column = right_out_put_schema->GetColumns();
                for (const auto &l_col : left_column) {
                    values.push_back(left_tuple.GetValue(left_out_put_schema, left_out_put_schema->GetColIdx(l_col.GetName())));
                }
                for (const auto &r_col : right_column) {
                    values.push_back(right_tuple.GetValue(right_out_put_schema, right_out_put_schema->GetColIdx(r_col.GetName())));
                }
                flag = true;
                left_tuple = Tuple(values, plan_out_put_schema);
            }
            *tuple = left_tuple;
            return true;
        }
        if (!flag) {
            break;
        }
    }
    return false; 
}

}  // namespace bustub
