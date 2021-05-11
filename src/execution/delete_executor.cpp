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
    table_metadata_->table_->MarkDelete(rid, exec_ctx_->GetTransaction());
    for (auto index_info : index_infos_) {
      auto key_attrs = index_info->index_->GetKeyAttrs();
      std::vector<Value> key_values(key_attrs.size());
      for (size_t i = 0; i < key_values.size(); i++) {
        key_values[i] = tuple.GetValue(&table_metadata_->schema_, key_attrs[i]);
      }
      Tuple key_tuple(key_values, &index_info->key_schema_);
      index_info->index_->DeleteEntry(key_tuple, rid, exec_ctx_->GetTransaction());
    }
  }

  return false;
}

}  // namespace bustub
