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
                AbstractExecutor(exec_ctx), plan_(plan),
                table_iter_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_->Begin(exec_ctx->GetTransaction())) 
                {}

void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_iter_ = table_info_->table_->Begin(GetExecutorContext()->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto the_end = table_info_->table_->End();
  auto table_schema = table_info_->schema_;
  auto predicate = plan_->GetPredicate();
  Tuple tmp_tuple;
  RID tmp_rid;
  const auto out_put_schema = plan_->OutputSchema();
  while(table_iter_ != the_end) {
    tmp_tuple = *table_iter_++;
    tmp_rid = tuple->GetRid();
    // predicate在数据库中一般用于where, in, 等用来判断对应的值where后面的值是否存在, 如果不存在这些则predicate为空
    if (predicate == nullptr || predicate->Evaluate(&tmp_tuple, &table_schema).GetAs<bool>()) {
      std::vector<Value> tmp_value;
      tmp_value.reserve(out_put_schema->GetColumnCount());
      for (size_t i = 0; i < tmp_value.capacity(); ++ i) {
        tmp_value.push_back(out_put_schema->GetColumn(i).GetExpr()->Evaluate(&tmp_tuple, &table_schema));
      }
      Tuple new_tuple(tmp_value, out_put_schema);
      *tuple = new_tuple;
      *rid = tmp_rid;
      return true;
    } 
  }
  return false;
}

}  // namespace bustub
