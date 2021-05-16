//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"
#include "execution/plans/index_scan_plan.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_metadata_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_metadata_->name_);
}

void InsertExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

void InsertExecutor::InternalInsertTuple(const Tuple &tuple) {
  RID rid;
  table_metadata_->table_->InsertTuple(tuple, &rid, exec_ctx_->GetTransaction());
  // TODO(tigertang): inappropriate position of locking
  exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), rid);
  for (auto index_info : index_infos_) {
    auto key_attrs = index_info->index_->GetKeyAttrs();
    std::vector<Value> key_values(key_attrs.size());
    for (size_t i = 0; i < key_values.size(); i++) {
      key_values[i] = tuple.GetValue(&table_metadata_->schema_, key_attrs[i]);
    }
    Tuple key_tuple(key_values, &index_info->key_schema_);
    index_info->index_->InsertEntry(tuple, rid, exec_ctx_->GetTransaction());
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *unused_tuple, RID *unused_rid) {
  if (plan_->IsRawInsert()) {
    for (size_t i = 0; i < plan_->RawValues().size(); i++) {
      auto raw_value = plan_->RawValuesAt(i);
      Tuple tuple(raw_value, &table_metadata_->schema_);
      InternalInsertTuple(tuple);
    }
    return false;
  }

  Tuple tuple;
  RID dummy_rid;
  while (child_executor_->Next(&tuple, &dummy_rid)) {
    InternalInsertTuple(tuple);
  }

  return false;
}

}  // namespace bustub
