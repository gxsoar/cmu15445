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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) :
                AbstractExecutor(exec_ctx), plan_{plan},
                table_iter_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_->Begin(exec_ctx->GetTransaction())) 
                {}

void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // Init();
  // auto table_iter = table_info_->table_->Begin(exec_ctx_->GetTransaction());
  auto the_end = table_info_->table_->End();
  auto predicate = plan_->GetPredicate();
  if (predicate == nullptr) {
    return false;
  }
  bool flag = false;
  auto schema = table_info_->schema_;
  while(table_iter_ != the_end) {
    auto get_tuple = *table_iter_++;
    auto get_rid = get_tuple.GetRid();
    if (predicate->Evaluate(&get_tuple, &schema).GetAs<bool>()) {
      *tuple = get_tuple;
      *rid = get_rid;
      flag = true;
      break;
    }
  }
  return flag;
}

}  // namespace bustub
