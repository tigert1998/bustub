//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  index_ = dynamic_cast<IndexType *>(index_info->index_.get());
  table_metadata_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
}

void IndexScanExecutor::Init() { iter_ = std::make_unique<IndexIteratorType>(index_->GetBeginIterator()); }

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (!iter_->isEnd()) {
    *rid = (**iter_).second;
    table_metadata_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
    iter_->operator++();

    auto predicate = plan_->GetPredicate();
    if (predicate == nullptr || predicate->Evaluate(tuple, &table_metadata_->schema_).GetAs<bool>()) {
      std::vector<Value> values;
      for (size_t i = 0; i < GetOutputSchema()->GetColumnCount(); i++) {
        auto expr = GetOutputSchema()->GetColumn(i).GetExpr();
        values.push_back(expr->Evaluate(tuple, &table_metadata_->schema_));
      }
      *tuple = Tuple(values, GetOutputSchema());

      return true;
    }
  }
  return false;
}

}  // namespace bustub
