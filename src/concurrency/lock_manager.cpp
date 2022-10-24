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

#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // std::lock_guard<std::mutex> lock(latch_);
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
  auto check = [&]() -> bool {
    for (auto ite = rq.begin(); ite != rq.end();) {
      if (ite->txn_id_ > txn->GetTransactionId()) {
        if (ite->lock_mode_ == LockMode::EXCLUSIVE) {
          if (ite->granted_) {
            Transaction *young_txn = TransactionManager::GetTransaction(ite->txn_id_);
            young_txn->SetState(TransactionState::ABORTED);
            rq.erase(ite);
            rid_lock_rq.cv_.notify_all();
            return true;
          }
          Transaction *young_txn = TransactionManager::GetTransaction(ite->txn_id_);
          young_txn->SetState(TransactionState::ABORTED);
          ite = rq.erase(ite);
        } else {
          ++ite;
        }
      } else {
        if (ite->lock_mode_ == LockMode::EXCLUSIVE && ite->granted_) {
          txn->SetState(TransactionState::ABORTED);
          rid_lock_rq.cv_.notify_all();
          return false;
        }
        ++ite;
      }
    }
    rid_lock_rq.cv_.notify_all();
    return true;
  };
  std::unique_lock<std::mutex> ulock(rid_lock_rq.mutex_);
  while (txn->GetState() != TransactionState::ABORTED && !check()) {
    lock_table_[rid].cv_.wait(ulock);
    if (txn->GetState() == TransactionState::ABORTED) {
      for (auto ite = rq.begin(); ite != rq.end(); ++ite) {
        if (ite->txn_id_ == txn->GetTransactionId()) {
          rq.erase(ite);
          break;
        }
      }
      rid_lock_rq.cv_.notify_all();
      return false;
    }
  }
  for (auto &ite : rid_lock_rq.request_queue_) {
    if (ite.txn_id_ == txn->GetTransactionId()) {
      ite.granted_ = true;
    }
  }
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
  // std::lock_guard<std::mutex> lock(latch_);
  auto &rq = lock_table_[rid].request_queue_;
  auto &rid_lock_rq = lock_table_[rid];
  rq.push_front(txn_request);
  auto check = [&]() -> bool {
    if (rq.size() == 1) {
      // rid_lock_rq.cv_.notify_all();
      return true;
    }
    // 如果前面有更老的事务占据锁
    for (auto ite = rq.begin(); ite != rq.end(); ++ite) {
      if (ite->txn_id_ < txn->GetTransactionId() && ite->granted_) {
        // txn->SetState(TransactionState::ABORTED);
        rid_lock_rq.cv_.notify_all();
        return false;
      }
    }
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
        ++ite;
      }
    }
    // 在处理后面的共享锁请求, 如果有多个较为年轻的共享锁占据一个tuple将这些共享锁终止掉
    size_t cnt_shared = 0;
    for (auto &ite : rq) {
      if (ite.txn_id_ > txn->GetTransactionId() && ite.lock_mode_ == LockMode::SHARED) {
        if (ite.granted_) {
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
      for (auto ite = rq.begin(); ite != rq.end() && cnt_shared != 0;) {
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
    rid_lock_rq.cv_.notify_all();
    return false;
  };
  std::unique_lock<std::mutex> ulock(rid_lock_rq.mutex_);
  while (txn->GetState() != TransactionState::ABORTED && !check()) {
    lock_table_[rid].cv_.wait(ulock);
    if (txn->GetState() == TransactionState::ABORTED) {
      for (auto ite = rq.begin(); ite != rq.end(); ++ite) {
        if (ite->txn_id_ == txn->GetTransactionId()) {
          rq.erase(ite);
          break;
        }
      }
      rid_lock_rq.cv_.notify_all();
      return false;
    }
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
  for (auto ite = rq.begin(); ite != rq.end();) {
    if (ite->txn_id_ == txn->GetTransactionId()) {
      ite = rq.erase(ite);
    } else {
      ++ite;
    }
  }
  txn->GetSharedLockSet()->erase(rid);
  bool ans = LockExclusive(txn, rid);
  return ans;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  auto &rq = lock_table_[rid].request_queue_;
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    for (auto ite = rq.begin(); ite != rq.end();) {
      if (ite->txn_id_ == txn->GetTransactionId()) {
        ite = rq.erase(ite);
        continue;
      } else {
        ++ite;
      }
    }
    txn->GetExclusiveLockSet()->erase(rid);
    txn->GetSharedLockSet()->erase(rid);
    lock_table_[rid].cv_.notify_all();
    return txn->GetState() == TransactionState::COMMITTED;
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
      lock_table_[rid].cv_.notify_all();
      return false;
    }
    for (auto ite = rq.begin(); ite != rq.end();) {
      if (ite->txn_id_ == txn->GetTransactionId()) {
        ite = rq.erase(ite);
        continue;
      } else {
        ++ite;
      }
    }
    lock_table_[rid].cv_.notify_all();
    return true;
  }
  lock_table_[rid].cv_.notify_all();
  return false;
}

bool LockManager::CanLock(Transaction *txn, LockMode mode) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (mode == LockMode::SHARED) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      TransactionAbortException e(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
      txn->SetState(TransactionState::ABORTED);
      throw e;
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
