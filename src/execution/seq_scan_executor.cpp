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

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto oid = plan_->GetTableOid();
  table_metadata_ = exec_ctx_->GetCatalog()->GetTable(oid);
}

void SeqScanExecutor::Init() {
  iter_ = std::make_unique<TableIterator>(table_metadata_->table_->Begin(exec_ctx_->GetTransaction()));
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (1) {
    if (*iter_ == table_metadata_->table_->End()) return false;
    *tuple = **iter_;
    auto res = plan_->GetPredicate()->Evaluate(tuple, &table_metadata_->schema_);
    if (res.IsNull() || res.GetAs<bool>()) {
      *rid = tuple->GetRid();
      iter_->operator++();
      return true;
    } else {
      iter_->operator++();
    }
  }
  return false;
}

}  // namespace bustub
