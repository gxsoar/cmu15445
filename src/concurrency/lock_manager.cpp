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
  LockRequest txn_request(txn->GetTransactionId(), LockMode::SHARED);
  // txn has locked rid
  if (txn->IsSharedLocked(rid)) {
    return true;
  }
  if (!CanLock(txn, LockMode::SHARED)) {
    return false;
  }
  // std::unique_lock<std::mutex> ulock(latch_);
  auto &rid_lock_rq = lock_table_[rid];
  auto &rq = lock_table_[rid].request_queue_;
  rq.push_back(txn_request);
  rid_lock_rq.count_shared++;
  auto check = [&]() -> bool {
    for (auto ite = rq.begin(); ite != rq.end(); ++ ite) {
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
      }
      ++ite;
    }
    LOG_INFO("cnt_shared = %lu rq.size = %lu\n", rid_lock_rq.count_shared, rq.size());
    return rid_lock_rq.count_shared == rq.size();
  };
  std::unique_lock<std::mutex> ulock(latch_);
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
  std::unique_lock<std::mutex> ulock(latch_);
  auto &rq = lock_table_[rid].request_queue_;
  auto &rid_lock_rq = lock_table_[rid];
  rq.push_front(txn_request);
  std::function<bool()> check = [&]() -> bool {
    if (rq.size() == 1) {
      return true;
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
        ++ ite;
      }
    }
    // 在处理后面的共享锁请求
    size_t cnt_shared = 0;
    for (auto ite = rq.begin(); ite != rq.end(); ++ ite) {
      if (ite->txn_id_ > txn->GetTransactionId() && ite->lock_mode_ == LockMode::SHARED) {
        if (ite->granted_) {
          cnt_shared++;
        }
      }
    }
    if (cnt_shared == 0) {
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
    return false;
  };
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
    return false;
  }
  auto &rid_lock_rq = lock_table_[rid];
  auto &rq = rid_lock_rq.request_queue_;
  for (auto ite = rq.begin(); ite != rq.end(); ++ ite) {
    if (ite->txn_id_ == txn->GetTransactionId()) {
      rq.erase(ite);
      break;
    }
  }
  txn->GetSharedLockSet()->erase(rid);
  // txn->GetExclusiveLockSet()->emplace(rid);
  return LockUpgrade(txn, rid);
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  bool flag = false;
  for (auto ite = lock_table_[rid].request_queue_.begin(); ite != lock_table_[rid].request_queue_.end(); ++ ite) {
    if (ite->txn_id_ == txn->GetTransactionId()) {
      if (ite->lock_mode_ == LockMode::SHARED) {
        lock_table_[rid].count_shared--;
      }
      if (ite->lock_mode_ == LockMode::EXCLUSIVE) {
        lock_table_[rid].count_exclusive--;
      }
      lock_table_[rid].request_queue_.erase(ite);
      break;
    }
  }
  lock_table_[rid].cv_.notify_all();
  if (txn->GetState() == TransactionState::GROWING) {
    if (txn->IsExclusiveLocked(rid)) {
      txn->SetState(TransactionState::SHRINKING);
      flag = true;
    } else if (txn->IsSharedLocked(rid) && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::SHRINKING);
      flag = true;
    }
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return flag;
}

bool LockManager::CanLock(Transaction *txn, LockMode mode) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (mode == LockMode::SHARED) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      TransactionAbortException e(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
      throw e;
      return false;
    }
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    TransactionAbortException e(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    throw e;
    return false;
  }
  return true;
}

}  // namespace bustub
