//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

#include "execution/expressions/column_value_expression.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  current_left_executor_ret_ = left_executor_->Next(&current_left_tuple_, &current_left_rid_);
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!current_left_executor_ret_) return false;
  Tuple right_tuple;
  RID right_rid;
  do {
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      auto ret = plan_->Predicate()->EvaluateJoin(&current_left_tuple_, plan_->GetLeftPlan()->OutputSchema(),
                                                  &right_tuple, plan_->GetRightPlan()->OutputSchema());
      if (ret.IsNull() || ret.GetAs<bool>()) {
        std::vector<Value> values;
        values.reserve(GetOutputSchema()->GetColumnCount());
        for (size_t i = 0; i < left_executor_->GetOutputSchema()->GetColumnCount(); i++) {
          values.push_back(current_left_tuple_.GetValue(left_executor_->GetOutputSchema(), i));
        }
        for (size_t i = 0; i < right_executor_->GetOutputSchema()->GetColumnCount(); i++) {
          values.push_back(right_tuple.GetValue(right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple(values, GetOutputSchema());
        return true;
      }
    }
    current_left_executor_ret_ = left_executor_->Next(&current_left_tuple_, &current_left_rid_);
    if (current_left_executor_ret_) {
      right_executor_->Init();
    }
  } while (current_left_executor_ret_);
  return false;
}

}  // namespace bustub
