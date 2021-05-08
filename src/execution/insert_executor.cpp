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
  next_raw_value_id_ = 0;
}

void InsertExecutor::InternalInsertTuple(const Tuple &tuple, RID *rid) {
  table_metadata_->table_->InsertTuple(tuple, rid, exec_ctx_->GetTransaction());
  for (auto index_info : index_infos_) {
    auto key_attrs = index_info->index_->GetKeyAttrs();
    std::vector<Value> key_values(key_attrs.size());
    for (size_t i = 0; i < key_values.size(); i++) {
      key_values[i] = tuple.GetValue(&table_metadata_->schema_, key_attrs[i]);
    }
    Tuple key_tuple(key_values, &index_info->key_schema_);
    index_info->index_->InsertEntry(tuple, *rid, exec_ctx_->GetTransaction());
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    if (next_raw_value_id_ == plan_->RawValues().size()) {
      return false;
    } else {
      auto raw_value = plan_->RawValuesAt(next_raw_value_id_);
      Tuple tuple(raw_value, &table_metadata_->schema_);
      InternalInsertTuple(tuple, rid);
      next_raw_value_id_++;
      return true;
    }
  } else {
    RID dummy_rid;
    if (!child_executor_->Next(tuple, &dummy_rid)) return false;
    InternalInsertTuple(*tuple, rid);
    return true;
  }
}

}  // namespace bustub
