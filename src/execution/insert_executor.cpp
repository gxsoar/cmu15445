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
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_(std::move(child_executor))
    {
    }

void InsertExecutor::Init() {
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    indexs_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    if (child_executor_) {
        child_executor_->Init();
    }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    auto schema = table_info_->schema_;
    auto table_heap = table_info_->table_.get();
    std::vector<Tuple> result_set;
    if (plan_->IsRawInsert()) {
        auto rawValues = plan_->RawValues();
        for (uint32_t i = 0; i < rawValues.size(); ++ i) {
            auto rawValue = plan_->RawValuesAt(i);
            Tuple tmp_tuple(rawValue, &schema);
            auto tmp_rid = tmp_tuple.GetRid();
            if (!table_heap->InsertTuple(tmp_tuple, &tmp_rid, exec_ctx_->GetTransaction())) {
                return false;
            }
            result_set.push_back(tmp_tuple);
        }    
    } else {
        Tuple tmp_tuple;
        RID tmp_rid;
        while(child_executor_->Next(&tmp_tuple, &tmp_rid)) {
            result_set.push_back(tmp_tuple);
        }
        for (auto &result_tuple : result_set) {
            auto tmp_rid = result_tuple.GetRid();
            if(!table_heap->InsertTuple(result_tuple, &tmp_rid, exec_ctx_->GetTransaction())) {
                return false;
            }
        }
    }
    for (auto &result_tuple : result_set) {
        for (auto &index_info : indexs_info_) {
            index_info->index_->InsertEntry(result_tuple, result_tuple.GetRid(), exec_ctx_->GetTransaction());
        }
    }
    return false;
}

}  // namespace bustub
