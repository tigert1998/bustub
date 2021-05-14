//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::ShouldGrantXLock(RID rid, txn_id_t tid) {
  auto &request = this->lock_table_[rid].request_queue_.front();
  if (request.txn_id_ == tid) {
    request.granted_ = true;
    return true;
  }
  return false;
}

bool LockManager::ShouldGrantSLock(RID rid, txn_id_t tid) {
  auto &request_queue = this->lock_table_[rid].request_queue_;

  for (auto &request : request_queue) {
    if (request.txn_id_ == tid) {
      request.granted_ = true;
      return true;
    }
    if (request.lock_mode_ == LockMode::EXCLUSIVE) {
      return false;
    }
  }

  return false;
}

bool LockManager::ShouldUpgradeLock(RID rid, txn_id_t tid) {
  auto &request_queue = this->lock_table_[rid].request_queue_;

  bool existed = false;
  decltype(request_queue.begin()) txn_iter;
  for (auto iter = request_queue.begin(); iter != request_queue.end(); iter++) {
    if (iter->lock_mode_ == LockMode::EXCLUSIVE) {
      if (!existed) {
        return false;
      }
      break;
    }
    if (iter->txn_id_ == tid) {
      txn_iter = iter;
      existed = true;
    } else if (iter->granted_) {
      return false;
    }
  }

  LockRequest lock_request = *txn_iter;
  lock_request.lock_mode_ = LockMode::EXCLUSIVE;
  lock_request.granted_ = true;
  request_queue.erase(txn_iter);
  request_queue.push_front(lock_request);
  return true;
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  txn->GetSharedLockSet()->emplace(rid);
  std::unique_lock lock(latch_);
  lock_table_[rid].request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
  if (ShouldGrantSLock(rid, txn->GetTransactionId())) {
    return true;
  }
  lock_table_[rid].cv_.wait(lock, [&, this]() { return this->ShouldGrantSLock(rid, txn->GetTransactionId()); });

  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  txn->GetExclusiveLockSet()->emplace(rid);
  std::unique_lock lock(latch_);
  if (ShouldGrantXLock(rid, txn->GetTransactionId())) {
    return true;
  }
  lock_table_[rid].cv_.wait(lock, [&, this]() { return this->ShouldGrantXLock(rid, txn->GetTransactionId()); });

  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  std::unique_lock lock(latch_);
  if (lock_table_[rid].upgrading_) {
    return false;
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);

  if (ShouldUpgradeLock(rid, txn->GetTransactionId())) {
    return true;
  }

  lock_table_[rid].upgrading_ = true;
  lock_table_[rid].cv_.wait(lock, [&, this]() { return this->ShouldUpgradeLock(rid, txn->GetTransactionId()); });

  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  if (txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  std::unique_lock lock(latch_);

  auto &request_queue = this->lock_table_[rid].request_queue_;
  auto iter = request_queue.begin();
  for (; iter != request_queue.end(); iter++) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      break;
    }
  }

  if (iter == request_queue.end()) {
    return false;
  }

  BUSTUB_ASSERT(iter->granted_,
                (std::string("Transaction ") + std::to_string(iter->txn_id_) + " is not granted.").c_str());
  request_queue.erase(iter);

  if (request_queue.empty()) {
    lock_table_.erase(rid);
  } else {
    lock_table_[rid].cv_.notify_all();
  }

  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

bool LockManager::HasCycle(txn_id_t *txn_id) { return false; }

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() { return {}; }

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      continue;
    }
  }
}

}  // namespace bustub
