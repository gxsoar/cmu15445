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
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexs_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  if (child_executor_) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
  LockManager *lgr = exec_ctx_->GetLockManager();
  auto schema = table_info_->schema_;
  auto table_heap = table_info_->table_.get();
  if (plan_->IsRawInsert()) {
    auto raw_values = plan_->RawValues();
    for (uint32_t i = 0; i < raw_values.size(); ++i) {
      auto raw_value = plan_->RawValuesAt(i);
      Tuple tmp_tuple(raw_value, &schema);
      auto tmp_rid = tmp_tuple.GetRid();
      if (!table_heap->InsertTuple(tmp_tuple, &tmp_rid, exec_ctx_->GetTransaction())) {
        return false;
      }
      if (!lgr->LockExclusive(txn, tmp_rid)) {
        txn_mgr->Abort(txn);
        return false;
      }
      for (auto &index_info : indexs_info_) {
        const auto index_key =
            tmp_tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(index_key, tmp_rid, exec_ctx_->GetTransaction());
        IndexWriteRecord iwr(tmp_rid, plan_->TableOid(), WType::INSERT, tmp_tuple, 
                            index_info->index_oid_, exec_ctx_->GetCatalog());
        txn->AppendTableWriteRecord(iwr);
      }
      
      
    }
    return false;
  }
  Tuple tmp_tuple;
  RID tmp_rid;
  std::vector<Tuple> result_set;
  while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
    if (!table_heap->InsertTuple(tmp_tuple, &tmp_rid, exec_ctx_->GetTransaction())) {
      return false;
    }
    if (!lgr->LockExclusive(txn, tmp_rid)) {
        txn_mgr->Abort(txn);
        return false;
    }
    for (const auto &index_info : indexs_info_) {
      const auto index_key = tmp_tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(index_key, tmp_rid, exec_ctx_->GetTransaction());
      IndexWriteRecord iwr(tmp_rid, plan_->TableOid(), WType::INSERT, tmp_tuple, 
                            index_info->index_oid_, exec_ctx_->GetCatalog());
      txn->AppendTableWriteRecord(iwr);
    }
  }
  return false;
}

}  // namespace bustub
