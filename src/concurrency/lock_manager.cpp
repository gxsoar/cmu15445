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
  std::unique_lock<std::mutex> ulock(latch_);
  LockRequest txn_request(txn->GetTransactionId(), LockMode::SHARED);
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
    if (rq.size() == 1) {
      return true;
    }
    for (auto ite = rq.begin(); ite != rq.end();) {
      if (ite->txn_id_ > txn->GetTransactionId()) {
        if (ite->lock_mode_ == LockMode::EXCLUSIVE) {
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
      } else {
        if (ite->lock_mode_ == LockMode::EXCLUSIVE && ite->granted_) {
          return false;
        }
        ++ite;
      }
    }
    rid_lock_rq.cv_.notify_all();
    return true;
  };
  while (txn->GetState() != TransactionState::ABORTED && !check()) {
    if (txn->GetState() == TransactionState::ABORTED) {
      for (auto ite = rq.begin(); ite != rq.end();) {
        if (ite->txn_id_ == txn->GetTransactionId()) {
          ite = rq.erase(ite);
        } else {
          ++ite;
        }
      }
      rid_lock_rq.cv_.notify_all();
      return false;
    }
    lock_table_[rid].cv_.wait(ulock);
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
  std::unique_lock<std::mutex> ulock(latch_);
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  if (!CanLock(txn, LockMode::EXCLUSIVE)) {
    return false;
  }
  LockRequest txn_request(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  auto &rq = lock_table_[rid].request_queue_;
  auto &rid_lock_rq = lock_table_[rid];
  rq.push_front(txn_request);
  auto check = [&]() -> bool {
    if (rq.size() == 1) {
      return true;
    }
    for (auto &ite : rq) {
      if (ite.lock_mode_ == LockMode::EXCLUSIVE && ite.txn_id_ < txn->GetTransactionId() && ite.granted_) {
        txn->SetState(TransactionState::ABORTED);
        return false;
      }
    }
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
    size_t cnt_shared = 0;
    for (auto &ite : rq) {
      if (ite.txn_id_ > txn->GetTransactionId() && ite.lock_mode_ == LockMode::SHARED) {
        if (ite.granted_) {
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
    return false;
  };
  while (txn->GetState() != TransactionState::ABORTED && !check()) {
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
    lock_table_[rid].cv_.wait(ulock);
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
  bool ans = LockExclusive(txn, rid);
  return ans;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
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
