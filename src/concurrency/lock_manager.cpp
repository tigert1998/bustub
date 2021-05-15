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

#include "concurrency/transaction_manager.h"

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

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  txn->GetSharedLockSet()->emplace(rid);
  std::unique_lock lock(latch_);
  lock_table_[rid].request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
  if (ShouldGrantSLock(rid, txn->GetTransactionId())) {
    return true;
  }
  lock_table_[rid].cv_.wait(lock, [&, this]() {
    return txn->GetState() == TransactionState::ABORTED || this->ShouldGrantSLock(rid, txn->GetTransactionId());
  });

  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }

  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  txn->GetExclusiveLockSet()->emplace(rid);
  std::unique_lock lock(latch_);
  lock_table_[rid].request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  if (ShouldGrantXLock(rid, txn->GetTransactionId())) {
    return true;
  }
  lock_table_[rid].cv_.wait(lock, [&, this]() {
    return txn->GetState() == TransactionState::ABORTED || this->ShouldGrantXLock(rid, txn->GetTransactionId());
  });

  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }

  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  std::unique_lock lock(latch_);
  if (lock_table_[rid].upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  lock_table_[rid].upgrading_ = true;

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);

  auto &request_queue = lock_table_[rid].request_queue_;
  decltype(request_queue.begin()) txn_iter, insert_iter = request_queue.end();
  for (auto iter = request_queue.begin(); iter != request_queue.end(); iter++) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      txn_iter = iter;
    } else if (iter->lock_mode_ == LockMode::EXCLUSIVE) {
      insert_iter = iter;
    }
  }
  request_queue.erase(txn_iter);
  request_queue.insert(insert_iter, LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE));

  if (ShouldGrantXLock(rid, txn->GetTransactionId())) {
    return true;
  }

  lock_table_[rid].cv_.wait(lock, [&, this]() {
    return txn->GetState() == TransactionState::ABORTED || this->ShouldGrantXLock(rid, txn->GetTransactionId());
  });

  lock_table_[rid].upgrading_ = false;

  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }

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

  request_queue.erase(iter);

  if (request_queue.empty()) {
    lock_table_.erase(rid);
  } else {
    lock_table_[rid].cv_.notify_all();
  }

  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  auto &vec = waits_for_[t1];
  auto iter = std::find(vec.begin(), vec.end(), t2);
  if (iter != vec.end()) {
    return;
  }
  vec.push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto &vec = waits_for_[t1];
  auto iter = std::find(vec.begin(), vec.end(), t2);
  if (iter == vec.end()) {
    return;
  }
  vec.erase(iter);
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  std::unordered_set<txn_id_t> visited, in_stack;
  std::deque<txn_id_t> stack;
  txn_id_t start;

  std::vector<txn_id_t> txn_ids;
  for (const auto &pair : waits_for_) {
    txn_ids.push_back(pair.first);
  }
  std::sort(txn_ids.begin(), txn_ids.end(), std::greater<txn_id_t>());

  for (auto x : txn_ids) {
    std::sort(waits_for_[x].begin(), waits_for_[x].end(), std::greater<txn_id_t>());
    if (visited.count(x)) {
      continue;
    }
    if (DFS(x, visited, in_stack, stack, &start)) {
      auto iter = std::find(stack.begin(), stack.end(), start);
      *txn_id = start;
      while (iter != stack.end()) {
        *txn_id = std::max(*txn_id, *iter);
        iter++;
      }
      return true;
    }
  }
  return false;
}

bool LockManager::DFS(txn_id_t x, std::unordered_set<txn_id_t> &visited, std::unordered_set<txn_id_t> &in_stack,
                      std::deque<txn_id_t> &stack, txn_id_t *start) {
  stack.push_back(x);
  visited.insert(x);
  in_stack.insert(x);
  for (auto y : waits_for_[x]) {
    if (in_stack.count(y)) {
      *start = y;
      return true;
    }
    if (visited.count(y)) {
      continue;
    }
    if (DFS(y, visited, in_stack, stack, start)) {
      return true;
    }
  }
  in_stack.erase(x);
  stack.pop_back();
  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (const auto &pair : waits_for_) {
    for (auto t2 : pair.second) {
      edges.push_back({pair.first, t2});
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      waits_for_.clear();

      // puts("==========");

      for (const auto &pair : lock_table_) {
        std::vector<txn_id_t> granted, not_granted;
        for (const auto &request : pair.second.request_queue_) {
          if (request.granted_) {
            granted.push_back(request.txn_id_);
          } else {
            not_granted.push_back(request.txn_id_);
          }
        }

        for (auto x : not_granted) {
          for (auto y : granted) {
            AddEdge(x, y);
            // printf("(%d, %d)\n", x, y);
          }
        }
      }

      txn_id_t txn_id;

      std::vector<txn_id_t> to_abort;

      while (HasCycle(&txn_id)) {
        auto edges = GetEdgeList();
        for (const auto &edge : edges) {
          if (edge.first == txn_id || edge.second == txn_id) {
            RemoveEdge(edge.first, edge.second);
          }
        }

        to_abort.push_back(txn_id);
      }

      // for (auto txn_id : to_abort) {
      // printf("%d, ", txn_id);
      // }
      // puts("");

      std::unordered_set<RID> rids;
      for (auto txn_id : to_abort) {
        auto txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);
        for (const auto &set : {*txn->GetSharedLockSet(), *txn->GetExclusiveLockSet()}) {
          for (RID rid : set) {
            auto &request_queue = lock_table_[rid].request_queue_;
            for (const auto &request : request_queue) {
              if (request.txn_id_ == txn_id && !request.granted_) {
                rids.insert(rid);
                goto next;
              }
            }
          }
        }

      next:;
      }

      for (auto rid : rids) {
        lock_table_[rid].cv_.notify_all();
      }
    }
  }
}

}  // namespace bustub
