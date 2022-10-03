//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)),
     aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
     aht_iterator_(aht_.Begin()), aht_iterator_end(aht_.End()){}

void AggregationExecutor::Init() {
    if (child_) {
        child_->Init();
    }
    Tuple tmp_tuple;
    RID tmp_rid;
    while(child_->Next(&tmp_tuple, &tmp_rid)) {
        AggregateKey agg_key = MakeAggregateKey(&tmp_tuple);
        AggregateValue agg_val = MakeAggregateValue(&tmp_tuple);
        aht_.InsertCombine(agg_key, agg_val);
    }
    aht_iterator_ = aht_.Begin();
    aht_iterator_end = aht_.End();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) { 
    if (aht_iterator_ == aht_iterator_end) {
        return false;
    }
    auto schema = plan_->OutputSchema();
    auto having = plan_->GetHaving();
    while(aht_iterator_ != aht_iterator_end) {
        AggregateKey agg_key = aht_iterator_.Key();
        AggregateValue agg_val = aht_iterator_.Val();
        ++aht_iterator_;
        if (having == nullptr || having->EvaluateAggregate(agg_key.group_bys_, agg_val.aggregates_).GetAs<bool>()) {
            std::vector<Value> vals;
            for (const auto &col : schema->GetColumns()) {
               vals.push_back(col.GetExpr()->EvaluateAggregate(agg_key.group_bys_, agg_val.aggregates_));
            }
            *tuple = Tuple(vals, schema);
            return true;
        }
    }
    return false; 
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
