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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_info_set_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  if (child_executor_) {
    child_executor_->Init();
  }
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  TableHeap *table_heap = table_info_->table_.get();
  Tuple tmp_tuple;
  RID tmp_rid;
  Schema schema = table_info_->schema_;
  Transaction *txn = GetExecutorContext()->GetTransaction();
  while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
    if (!table_heap->MarkDelete(tmp_rid, txn)) {
      return false;
    }
    for (auto &index_info : index_info_set_) {
      Tuple index_key =
          tmp_tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetMetadata()->GetKeyAttrs());
      index_info->index_->DeleteEntry(index_key, tmp_rid, txn);
    }
  }
  return false;
}

}  // namespace bustub
