//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()) {
  auto output_schema = GetOutputSchema();
  auto &group_bys = plan_->GetGroupBys();
  auto &aggregates = plan_->GetAggregates();
  for (size_t i = 0; i < output_schema->GetColumnCount(); i++) {
    auto expr = output_schema->GetColumn(i).GetExpr();
    auto iter = std::find(group_bys.begin(), group_bys.end(), expr);
    if (iter != group_bys.end()) {
      attrs_.emplace_back(0, iter - group_bys.begin());
      continue;
    }
    iter = std::find(aggregates.begin(), aggregates.end(), expr);
    attrs_.emplace_back(1, iter - aggregates.begin());
  }
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    auto agg_key = MakeKey(&tuple);
    auto agg_value = MakeVal(&tuple);
    aht_.InsertCombine(agg_key, agg_value);
  }

  aht_iterator_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_.Begin());
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (*aht_iterator_ != aht_.End()) {
    // group by
    auto key = aht_iterator_->Key();
    // aggregate
    auto value = aht_iterator_->Val();

    std::vector<Value> values;
    for (auto attr : attrs_) {
      if (attr.first == 0) {
        values.emplace_back(key.group_bys_[attr.second]);
      } else {
        values.emplace_back(value.aggregates_[attr.second]);
      }
    }
    *tuple = Tuple(values, GetOutputSchema());
    auto ret = plan_->GetHaving()->Evaluate(tuple, GetOutputSchema());

    aht_iterator_->operator++();
    if (ret.IsNull() || ret.GetAs<bool>()) {
      return true;
    }
  }

  return false;
}

}  // namespace bustub
