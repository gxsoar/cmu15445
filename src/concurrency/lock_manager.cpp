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

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::lock_guard<std::mutex> lock(latch_);
  // std::unique_lock<std::mutex> ulock(latch_);
  LockRequest txn_request(txn->GetTransactionId(), LockMode::SHARED);
  // txn has locked rid
  if (txn->IsSharedLocked(rid)) {
    return true;
  }
  if (!CanLock(txn, LockMode::SHARED)) {
    return false;
  }
  auto &rid_lock_rq = lock_table_[rid];
  auto &rq = lock_table_[rid].request_queue_;
  rq.push_back(txn_request);
  rid_lock_rq.count_shared++;
  auto check = [&]() -> bool {
    for (auto ite = rq.begin(); ite != rq.end();) {
      if (ite->txn_id_ > txn->GetTransactionId()) {
        if (ite->lock_mode_ == LockMode::EXCLUSIVE) {
          if (ite->granted_) {
            Transaction* young_txn = TransactionManager::GetTransaction(ite->txn_id_);
            young_txn->SetState(TransactionState::ABORTED);
            rq.erase(ite);
            rid_lock_rq.count_exclusive--;
            rid_lock_rq.cv_.notify_all();
            return true;
          } else {
            Transaction* young_txn = TransactionManager::GetTransaction(ite->txn_id_);
            young_txn->SetState(TransactionState::ABORTED);
            ite = rq.erase(ite);
            rid_lock_rq.count_exclusive--;
            continue;
          }
        }
      } else {
        if (ite->lock_mode_ == LockMode::EXCLUSIVE && ite->granted_) {
          return false;
        }
      }
      ++ite;
    }
    rid_lock_rq.cv_.notify_all();
    return true;
  };
  std::unique_lock<std::mutex> ulock(lock_table_[rid].mutex_);
  while(txn->GetState() != TransactionState::ABORTED && !check()) {
    lock_table_[rid].cv_.wait(ulock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    for (auto ite = rq.begin(); ite != rq.end(); ++ ite) {
      if (ite->txn_id_ == txn->GetTransactionId()) {
        rq.erase(ite);
        break;
      }
    }
    rid_lock_rq.cv_.notify_all();
    return false;
  }
  for (auto &ite : rid_lock_rq.request_queue_) {
    if (ite.txn_id_ == txn->GetTransactionId()) {
      ite.granted_ = true;
    }
  }
  // LOG_INFO("rq.size() = %ld\n", rid_lock_rq.request_queue_.size());
  txn->SetState(TransactionState::GROWING);
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  if (!CanLock(txn, LockMode::EXCLUSIVE)) {
    return false;
  }
  LockRequest txn_request(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  std::lock_guard<std::mutex> lock(latch_);
  // std::unique_lock<std::mutex> ulock(latch_);
  auto &rq = lock_table_[rid].request_queue_;
  auto &rid_lock_rq = lock_table_[rid];
  rq.push_front(txn_request);
  std::function<bool()> check = [&]() -> bool {
    if (rq.size() == 1) {
      return true;
    }
    // LOG_INFO("rq.size() = %lu\n", rq.size());
    // 先处理前面的排他锁请求
    for (auto ite = rq.begin(); ite != rq.end();) {
      if (ite->txn_id_ > txn->GetTransactionId() && ite->lock_mode_ == LockMode::EXCLUSIVE) {
        Transaction *young_txn = TransactionManager::GetTransaction(ite->txn_id_);
        young_txn->SetState(TransactionState::ABORTED);
        if (ite->granted_) {
          rq.erase(ite);
          rid_lock_rq.cv_.notify_all();
          return true;
        }
        ite = rq.erase(ite); 
      } else {
        ++ ite;
      }
    }
    // 在处理后面的共享锁请求, 如果有多个较为年轻的共享锁占据一个tuple将这些共享锁终止掉
    size_t cnt_shared = 0;
    for (auto ite = rq.begin(); ite != rq.end(); ++ ite) {
      if (ite->txn_id_ > txn->GetTransactionId() && ite->lock_mode_ == LockMode::SHARED) {
        if (ite->granted_) {
          cnt_shared++;
        }
      }
    }
    if (cnt_shared == 0) {
      // 将后面年轻的正在等待的共享锁的txn都给终止掉
      for (auto ite = rq.begin(); ite != rq.end();) {
        if (ite->txn_id_ > txn->GetTransactionId() && ite->lock_mode_ == LockMode::SHARED) {
          Transaction *young_txn = TransactionManager::GetTransaction(ite->txn_id_);
          young_txn->SetState(TransactionState::ABORTED);
          ite = rq.erase(ite);
        } else {
          ++ite;
        }
      }
    } else {
      for (auto ite = rq.begin(); ite != rq.end(); ) {
        if (ite->txn_id_ > txn->GetTransactionId() && ite->lock_mode_ == LockMode::SHARED && ite->granted_) {
          Transaction *young_txn = TransactionManager::GetTransaction(ite->txn_id_);
          young_txn->SetState(TransactionState::ABORTED);
          ite = rq.erase(ite);
          cnt_shared--;
        } else {
          ++ite;
        }
      }
      if (cnt_shared == 0) {
        rid_lock_rq.cv_.notify_all();
        return true;
      }
    }
    // LOG_INFO("deadlock!!!\n");
    return false;
  };
  std::unique_lock<std::mutex> ulock(rid_lock_rq.mutex_);
  while(txn->GetState() != TransactionState::ABORTED && !check()) {
    lock_table_[rid].cv_.wait(ulock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    for (auto ite = rq.begin(); ite != rq.end(); ++ ite) {
      if (ite->txn_id_ == txn->GetTransactionId()) {
        rq.erase(ite);
        break;
      }
    }
    return false;
  }
  for (auto &ite : rq) {
    if (ite.txn_id_ == txn->GetTransactionId()) {
      ite.granted_ = true;
    }
  }
  txn->SetState(TransactionState::GROWING);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (txn->IsExclusiveLocked(rid) || !txn->IsSharedLocked(rid)) {
    return true;
  }
  auto &rid_lock_rq = lock_table_[rid];
  auto &rq = rid_lock_rq.request_queue_;
  // LOG_INFO("shared_size = %lu exclusive_size = %lu rq size = %lu\n",txn->GetSharedLockSet()->size(), txn->GetExclusiveLockSet()->size(), lock_table_[rid].request_queue_.size());
  for (auto ite = rq.begin(); ite != rq.end(); ) {
    if (ite->txn_id_ == txn->GetTransactionId()) {
      ite = rq.erase(ite);
    } else ++ite;
  }
  // LOG_INFO("rq size = %lu\n", lock_table_[rid].request_queue_.size());
  txn->GetSharedLockSet()->erase(rid);
  bool ans = LockExclusive(txn, rid);
  // txn->GetExclusiveLockSet()->emplace(rid);
  return ans;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::lock_guard<std::mutex> lock(latch_);
  if (txn->GetState() == TransactionState::ABORTED) {
    txn->GetExclusiveLockSet()->erase(rid);
    txn->GetSharedLockSet()->erase(rid);
    for (auto ite = lock_table_[rid].request_queue_.begin(); ite != lock_table_[rid].request_queue_.end();++ ite) {
      if (ite->txn_id_ == txn->GetTransactionId()) {
        lock_table_[rid].request_queue_.erase(ite);
        break;
      }
    }
    return false;
  }
  if (txn->GetState() == TransactionState::GROWING || txn->GetState() == TransactionState::SHRINKING) {
    if (txn->IsExclusiveLocked(rid)) {
        txn->SetState(TransactionState::SHRINKING);
        txn->GetExclusiveLockSet()->erase(rid);
      } else if (txn->IsSharedLocked(rid)) {
        txn->GetSharedLockSet()->erase(rid);
        if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
          txn->SetState(TransactionState::SHRINKING);
        }
      } else {
        return false;
      } 
    for (auto ite = lock_table_[rid].request_queue_.begin(); ite != lock_table_[rid].request_queue_.end();) {
      if (ite->txn_id_ == txn->GetTransactionId()) {
        ite = lock_table_[rid].request_queue_.erase(ite);
        break;
      } else {
        ++ ite;
      }
    }
    lock_table_[rid].cv_.notify_all();
    return true;
  }
  return false;
  
  // return true;
}

bool LockManager::CanLock(Transaction *txn, LockMode mode) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (mode == LockMode::SHARED) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      TransactionAbortException e(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
      throw e;
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    TransactionAbortException e(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    txn->SetState(TransactionState::ABORTED);
    throw e;
    return false;
  }
  return true;
}

}  // namespace bustub
