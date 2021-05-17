//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_metadata_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_metadata_->name_);
}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *unused_tuple, RID *unused_rid) {
  Tuple tuple;
  RID rid;

  while (child_executor_->Next(&tuple, &rid)) {
    if (exec_ctx_->GetTransaction()->IsSharedLocked(rid)) {
      exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), rid);
    } else {
      exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), rid);
    }
    table_metadata_->table_->MarkDelete(rid, exec_ctx_->GetTransaction());
    for (auto index_info : index_infos_) {
      auto key_tuple =
          tuple.KeyFromTuple(table_metadata_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());

      exec_ctx_->GetTransaction()->AppendIndexWriteRecord(IndexWriteRecord(
          rid, table_metadata_->oid_, WType::DELETE, tuple, index_info->index_oid_, exec_ctx_->GetCatalog()));
      index_info->index_->DeleteEntry(key_tuple, rid, exec_ctx_->GetTransaction());
    }
  }

  return false;
}

}  // namespace bustub
