//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  if (child_executor_) {
    child_executor_->Init();
  }
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  TableHeap *table_heap = table_info_->table_.get();
  auto indexs_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  Tuple tmp_tuple;
  RID tmp_rid;
  Schema schema = table_info_->schema_;
  while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
    if (!table_heap->GetTuple(tmp_rid, &tmp_tuple, exec_ctx_->GetTransaction())) {
      return false;
    }
    Tuple new_tuple = GenerateUpdatedTuple(tmp_tuple);
    if (table_heap->UpdateTuple(new_tuple, tmp_rid, exec_ctx_->GetTransaction())) {
      RID new_rid = tmp_rid;
      for (auto &index_info : indexs_info) {
        auto old_index_key = tmp_tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(old_index_key, tmp_rid, exec_ctx_->GetTransaction());
        auto new_index_key = new_tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(new_index_key, new_rid, exec_ctx_->GetTransaction());
      }
    }
  }
  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
