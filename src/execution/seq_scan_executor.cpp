//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_iter_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_->Begin(exec_ctx->GetTransaction())) {}

void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_iter_ = table_info_->table_->Begin(GetExecutorContext()->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
  LockManager *lgr = exec_ctx_->GetLockManager();
  auto the_end = table_info_->table_->End();
  auto table_schema = table_info_->schema_;
  auto predicate = plan_->GetPredicate();
  Tuple tmp_tuple;
  RID tmp_rid;
  const auto out_put_schema = plan_->OutputSchema();
  while (table_iter_ != the_end) {
    tmp_tuple = *table_iter_++;
    tmp_rid = tmp_tuple.GetRid();
    if (!lgr->LockShared(txn, tmp_rid)) {
      txn_mgr->Abort(txn);
      return false;
    }
    // predicate在数据库中一般用于where, in, 等用来判断对应的值where后面的值是否存在, 如果不存在这些则predicate为空
    if (predicate == nullptr || predicate->Evaluate(&tmp_tuple, &table_schema).GetAs<bool>()) {
      const auto columns = out_put_schema->GetColumns();
      std::vector<Value> tmp_value;
      tmp_value.resize(columns.size());
      size_t i = 0;
      for (const auto &col : columns) {
        tmp_value[i++] = col.GetExpr()->Evaluate(&tmp_tuple, &table_schema);
      }
      Tuple new_tuple(tmp_value, out_put_schema);
      *tuple = new_tuple;
      *rid = tmp_rid;
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        if (!lgr->Unlock(txn, tmp_rid)) {
          txn_mgr->Abort(txn);
          return false;
        }
      }
      return true;
    }
  }
  return false;
}

}  // namespace bustub
