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

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  TransactionState txn_state = txn->GetState();
  IsolationLevel txn_isolate_lv = txn->GetIsolationLevel();
  LockRequest txn_request(txn->GetTransactionId(), LockMode::SHARED);
  // txn has locked rid
  if (txn->IsSharedLocked(rid)) {
    return true;
  }
  if (!CanLock(txn, LockMode::SHARED)) {
    return false;
  }
  std::unique_lock<std::mutex> ulock(latch_);
  auto &rid_lock_rq = lock_table_[rid];
  auto &rq = lock_table_[rid].request_queue_;
  rq.push_back(txn_request);
  auto check = [&]() -> bool {
    size_t cnt_shared = 0;
    for (auto ite = rq.begin(); ite != rq.end(); ++ ite) {
      if (ite->txn_id_ > txn->GetTransactionId()) {
        if (ite->granted_) {
          if (ite->lock_mode_ == LockMode::EXCLUSIVE) {
            Transaction* young_txn = TransactionManager::GetTransaction(ite->txn_id_);
            young_txn->SetState(TransactionState::ABORTED);
            rq.erase(ite);
            rid_lock_rq.cv_.notify_all();
            return true;
          }
        } else {
          Transaction* young_txn = TransactionManager::GetTransaction(ite->txn_id_);
          young_txn->SetState(TransactionState::ABORTED);
          ite = rq.erase(ite);
          continue;
        }
      }
      if (ite->lock_mode_ == LockMode::SHARED) {
        cnt_shared++;
      }
      ++ite;
    }
    return cnt_shared == rq.size();
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

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
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
