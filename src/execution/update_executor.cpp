//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *unused_tuple, RID *unused_rid) {
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    auto new_tuple = GenerateUpdatedTuple(tuple);
    if (exec_ctx_->GetTransaction()->IsSharedLocked(rid)) {
      exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), rid);
    } else {
      exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), rid);
    }
    table_info_->table_->UpdateTuple(new_tuple, rid, exec_ctx_->GetTransaction());
    // TODO(tigertang): update index
  }
  return false;
}
}  // namespace bustub
