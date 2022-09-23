//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, 
    // table_iter_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())->table_->Begin(exec_ctx->GetTransaction())),
    table_heap_(exec_ctx->GetBufferPoolManager(), exec_ctx->GetLockManager(), exec_ctx->GetLogManager(), exec_ctx->GetTransaction())
    {}

void InsertExecutor::Init() {
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    // table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
    indexs_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    auto schema = table_info_->schema_;
    bool flag = false;
    if (plan_->IsRawInsert()) {
        auto rawValues = plan_->RawValues();
        for (uint32_t i = 0; i < rawValues.size(); ++ i) {
            auto rawValue = plan_->RawValuesAt(i);
            Tuple tmp_tuple(rawValue, &schema);
            auto tmp_rid = tmp_tuple.GetRid();
            flag = table_heap_.InsertTuple(tmp_tuple, &tmp_rid, exec_ctx_->GetTransaction());
        }
    } else {
        auto child_plan = plan_->GetChildPlan();
        auto executor = ExecutorFactory::CreateExecutor(exec_ctx_, child_plan);
        Tuple tmp_tuple;
        RID tmp_rid;
        executor->Init();
        std::vector<Tuple> result_set;
        while(executor->Next(&tmp_tuple, &tmp_rid)) {
            result_set.push_back(tmp_tuple);
        }
        if (result_set.empty()) {
            return false;
        }
        for (auto &result_tuple : result_set) {
            auto tmp_rid = result_tuple.GetRid();
            flag = table_heap_.InsertTuple(tmp_tuple, &tmp_rid, exec_ctx_->GetTransaction());
        }
    }
    for (auto &index : indexs_info_) {
        
    }
    return flag; 
}

}  // namespace bustub
