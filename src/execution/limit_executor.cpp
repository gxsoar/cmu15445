//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  if (child_executor_) {
    child_executor_->Init();
  }
  limits_ = 0;
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  auto plan_limit = plan_->GetLimit();
  if (limits_ == plan_limit) {
    return false;
  }
  while (child_executor_->Next(tuple, rid) && limits_ < plan_limit) {
    ++limits_;
    return true;
  }
  return false;
}

}  // namespace bustub
