//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_plan.h
//
// Identification: src/include/execution/plans/distinct_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/plans/abstract_plan.h"
#include "common/util/hash_util.h"

namespace bustub {

/**
 * Distinct removes duplicate rows from the output of a child node.
 */
class DistinctPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new DistinctPlanNode instance.
   * @param child The child plan from which tuples are obtained
   */
  DistinctPlanNode(const Schema *output_schema, const AbstractPlanNode *child)
      : AbstractPlanNode(output_schema, {child}) {}

  /** @return The type of the plan node */
  PlanType GetType() const override { return PlanType::Distinct; }

  /** @return The child plan node */
  const AbstractPlanNode *GetChildPlan() const {
    BUSTUB_ASSERT(GetChildren().size() == 1, "Distinct should have at most one child plan.");
    return GetChildAt(0);
  }
};

struct DistinctKey {
  std::vector<Value> vals;
  bool operator==(const DistinctKey &other) const {
    for (uint i = 0; i < other.vals.size(); ++ i) {
      if (vals[i].CompareEquals(other.vals[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

namespace std {

template<>
struct hash<bustub::DistinctKey> {
  std::size_t operator() (const bustub::DistinctKey &dist_key) const {
    size_t curr_hash = 0;
    auto vals = dist_key.vals;
    for (const auto &val : vals) {
      if (!val.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&val));
      }
    }
    return curr_hash;
  }
};
} // namespace std