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
  LockRequest txn_request(txn->GetTransactionId(), LockMode::SHARED);
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
    bool flag = true;
    if (rq.size() == 1) {
      return true;
    }
    for (auto ite = rq.begin(); ite != rq.end();) {
      if (ite->lock_mode_ == LockMode::EXCLUSIVE) {
        if (ite->txn_id_ > txn->GetTransactionId()) {
          Transaction *young_txn = TransactionManager::GetTransaction(ite->txn_id_);
          young_txn->SetState(TransactionState::ABORTED);
          ite = rq.erase(ite);
          rid_lock_rq.cv_.notify_all();
        } else if (ite->txn_id_ < txn->GetTransactionId()) {
          flag = false;
          ++ite;
        } else {
          ++ite;
        }
      } else {
        ++ite;
      }
    }
    return txn->GetState() != TransactionState::ABORTED && flag;
  };
  while (!check() && txn->GetState() != TransactionState::ABORTED) {
    lock_table_[rid].cv_.wait(ulock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    for (auto ite = rq.begin(); ite != rq.end();) {
      if (ite->txn_id_ == txn->GetTransactionId()) {
        ite = rq.erase(ite);
      } else {
        ++ite;
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
  std::unique_lock<std::mutex> ulock(latch_);
  LockRequest txn_request(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  auto &rq = lock_table_[rid].request_queue_;
  auto &rid_lock_rq = lock_table_[rid];
  rq.push_back(txn_request);
  auto check = [&]() -> bool {
    bool flag = true;
    if (rq.size() == 1) {
      return true;
    }
    for (auto ite = rq.begin(); ite != rq.end();) {
      if (ite->txn_id_ < txn->GetTransactionId()) {
        flag = false;
        ++ite;
      } else if (ite->txn_id_ > txn->GetTransactionId()) {
        Transaction *young_txn = TransactionManager::GetTransaction(ite->txn_id_);
        young_txn->SetState(TransactionState::ABORTED);
        ite = rq.erase(ite);
        rid_lock_rq.cv_.notify_all();
      } else {
        ++ite;
      }
    }
    return txn->GetState() != TransactionState::ABORTED && flag;
  };
  while (!check() && txn->GetState() != TransactionState::ABORTED) {
    lock_table_[rid].cv_.wait(ulock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    for (auto ite = rq.begin(); ite != rq.end();) {
      if (ite->txn_id_ == txn->GetTransactionId()) {
        ite = rq.erase(ite);
      } else {
        ++ite;
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
  std::unique_lock<std::mutex> ulock(latch_);
  if (txn->GetState() != TransactionState::GROWING) {
    TransactionAbortException e(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    throw e;
    return false;
  }
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
  ulock.unlock();
  bool ans = LockExclusive(txn, rid);
  return ans;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> ulock(latch_);
  auto &rq = lock_table_[rid].request_queue_;
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    for (auto ite = rq.begin(); ite != rq.end();) {
      if (ite->txn_id_ == txn->GetTransactionId()) {
        ite = rq.erase(ite);
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
      return false;
    }
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    TransactionAbortException e(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  return true;
}

}  // namespace bustub
