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
#include "execution/expressions/aggregate_value_expression.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()) {}

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

    auto having = plan_->GetHaving();
    bool ret = having == nullptr || having->EvaluateAggregate(key.group_bys_, value.aggregates_).GetAs<bool>();

    aht_iterator_->operator++();
    if (ret) {
      std::vector<Value> values;
      for (size_t i = 0; i < GetOutputSchema()->GetColumnCount(); i++) {
        values.emplace_back(
            GetOutputSchema()->GetColumn(i).GetExpr()->EvaluateAggregate(key.group_bys_, value.aggregates_));
      }
      *tuple = Tuple(values, GetOutputSchema());
      return true;
    }
  }

  return false;
}

}  // namespace bustub
