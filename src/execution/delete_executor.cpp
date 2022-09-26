//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    if (child_executor_) {
        child_executor_->Init();
    }
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    TableHeap* table_heap = table_info_->table_.get();
    auto indexs_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    std::vector<std::pair<Tuple,RID>> delete_sets;
    Tuple tmp_tuple;
    RID tmp_rid;
    Schema schema = table_info_->schema_;
    while(child_executor_->Next(&tmp_tuple, &tmp_rid)) {
        delete_sets.push_back(std::make_pair(tmp_tuple, tmp_rid));
        table_heap->MarkDelete(tmp_rid, exec_ctx_->GetTransaction());
    }
    // std::cout << delete_sets.size() << std::endl;
    int cnt = 0;
    for (auto &delete_ele : delete_sets) {
        Tuple delete_tuple = delete_ele.first;
        RID delete_rid = delete_ele.second;
        // table_heap->ApplyDelete(delete_rid, exec_ctx_->GetTransaction());
        if (table_heap->MarkDelete(delete_rid, exec_ctx_->GetTransaction())) {
            table_heap->ApplyDelete(delete_rid, exec_ctx_->GetTransaction());
            std::cout << ++cnt << std::endl;
            for (auto &index_info : indexs_info) {
                Tuple index_key = delete_tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
                index_info->index_->DeleteEntry(index_key, delete_rid, exec_ctx_->GetTransaction());
            }
        } else {
            return false;
        }
    }
    return false; 
}

}  // namespace bustub
