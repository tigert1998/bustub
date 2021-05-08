//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

#include "execution/expressions/column_value_expression.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto oid = plan_->GetTableOid();
  table_metadata_ = exec_ctx_->GetCatalog()->GetTable(oid);
  attrs_.resize(GetOutputSchema()->GetColumnCount());
  for (size_t i = 0; i < attrs_.size(); i++) {
    auto expr = dynamic_cast<const ColumnValueExpression *>(GetOutputSchema()->GetColumn(i).GetExpr());
    attrs_[i] = expr->GetColIdx();
  }
}

void SeqScanExecutor::Init() {
  iter_ = std::make_unique<TableIterator>(table_metadata_->table_->Begin(exec_ctx_->GetTransaction()));
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (1) {
    if (*iter_ == table_metadata_->table_->End()) return false;
    *tuple = **iter_;
    auto predicate = plan_->GetPredicate();
    bool res = predicate == nullptr || predicate->Evaluate(tuple, &table_metadata_->schema_).GetAs<bool>();

    if (res) {
      *rid = tuple->GetRid();
      std::vector<Value> values;
      for (size_t i = 0; i < attrs_.size(); i++)
        values.push_back(tuple->GetValue(&table_metadata_->schema_, attrs_[i]));
      *tuple = Tuple(values, GetOutputSchema());

      iter_->operator++();
      return true;
    } else {
      iter_->operator++();
    }
  }
  return false;
}

}  // namespace bustub
