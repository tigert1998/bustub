//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

#include "execution/expressions/comparison_expression.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  inner_table_metadata_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  inner_index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), inner_table_metadata_->name_);

  auto predicate = dynamic_cast<const ComparisonExpression *>(plan_->Predicate());
  left_col_value_expr_ = dynamic_cast<const ColumnValueExpression *>(predicate->GetChildAt(0));
  right_col_value_expr_ = dynamic_cast<const ColumnValueExpression *>(predicate->GetChildAt(1));
  BUSTUB_ASSERT(left_col_value_expr_ != nullptr && right_col_value_expr_ != nullptr, "");
  BUSTUB_ASSERT(left_col_value_expr_->GetTupleIdx() == 0 && right_col_value_expr_->GetTupleIdx() == 1, "");
  auto key_attrs = inner_index_info_->index_->GetKeyAttrs();
  BUSTUB_ASSERT(right_col_value_expr_->GetColIdx() == key_attrs[0] && key_attrs.size() == 1, "");
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple outer_tuple;
  RID outer_rid;

  while (child_executor_->Next(&outer_tuple, &outer_rid)) {
    std::vector<Value> key_values(1);
    key_values[0] = left_col_value_expr_->Evaluate(&outer_tuple, plan_->OuterTableSchema());
    Tuple key_tuple(key_values, &inner_index_info_->key_schema_);

    std::vector<RID> rids;
    inner_index_info_->index_->ScanKey(key_tuple, &rids, exec_ctx_->GetTransaction());

    if (!rids.empty()) {
      BUSTUB_ASSERT(rids.size() == 1, "");
      Tuple inner_tuple;
      inner_table_metadata_->table_->GetTuple(rids[0], &inner_tuple, exec_ctx_->GetTransaction());

      std::vector<Value> values;
      for (size_t i = 0; i < GetOutputSchema()->GetColumnCount(); i++) {
        auto expr = GetOutputSchema()->GetColumn(i).GetExpr();
        values.push_back(
            expr->EvaluateJoin(&outer_tuple, plan_->OuterTableSchema(), &inner_tuple, plan_->InnerTableSchema()));
      }
      *tuple = Tuple(values, GetOutputSchema());

      return true;
    }
  }
  return false;
}

}  // namespace bustub
