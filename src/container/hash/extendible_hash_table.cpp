
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  table_latch_.WLock();
  directory_page_id_ = INVALID_PAGE_ID;
  HashTableDirectoryPage *dir_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager->NewPage(&directory_page_id_, nullptr));
  dir_page->SetPageId(directory_page_id_);
  auto bucket_page_id = INVALID_PAGE_ID;
  buffer_pool_manager->NewPage(&bucket_page_id, nullptr);
  dir_page->SetBucketPageId(0, bucket_page_id);
  buffer_pool_manager->UnpinPage(directory_page_id_, true, nullptr);
  buffer_pool_manager->UnpinPage(bucket_page_id, true, nullptr);
  table_latch_.WUnlock();
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  auto hash_value = Hash(key);
  auto global_mask = dir_page->GetGlobalDepthMask();
  auto dir_idx = hash_value & global_mask;
  return dir_page->GetBucketPageId(dir_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  HashTableDirectoryPage *fetch_dir_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
  return fetch_dir_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  HASH_TABLE_BUCKET_TYPE *bucket_page = reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(
      buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
  return bucket_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  auto dir_idx = KeyToDirectoryIndex(key, dir_page);
  auto bucket_page_id = dir_page->GetBucketPageId(dir_idx);
  HASH_TABLE_BUCKET_TYPE *get_bucket_page = FetchBucketPage(bucket_page_id);
  bool flag = get_bucket_page->GetValue(key, comparator_, result);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  table_latch_.RUnlock();
  return flag;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  auto bucket_idx = KeyToDirectoryIndex(key, dir_page);
  auto bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  HASH_TABLE_BUCKET_TYPE *get_bucket_page = FetchBucketPage(bucket_page_id);
  bool ans = false;
  if (get_bucket_page->IsFull()) {
    ans = SplitInsert(transaction, key, value);
  } else {
    ans = get_bucket_page->Insert(key, value, comparator_);
  }
  table_latch_.WUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  return ans;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto dir_page = FetchDirectoryPage();
  auto bucket_idx = KeyToDirectoryIndex(key, dir_page);
  std::vector<std::pair<KeyType, ValueType>> res;
  auto bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  bucket_page->GetAllValue(&res);
  bucket_page->ClearBucket();
  if (dir_page->GetLocalDepth(bucket_idx) == dir_page->GetGlobalDepth()) {
    dir_page->IncrLocalDepth(bucket_idx);
    dir_page->IncrGlobalDepth();
  } else {
    dir_page->IncrLocalDepth(bucket_idx);
  }
  auto split_bucket_page_idx = dir_page->GetSplitImageIndex(bucket_idx);
  auto split_bucket_page_id = INVALID_PAGE_ID;
  HashTableBucketPage<KeyType, ValueType, KeyComparator> *split_bucket_page =
      reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(
          buffer_pool_manager_->NewPage(&split_bucket_page_id, nullptr)->GetData());
  dir_page->SetBucketPageId(split_bucket_page_idx, split_bucket_page_id);
  dir_page->SetLocalDepth(split_bucket_page_idx, dir_page->GetLocalDepth(bucket_idx));
  //  重新分配原来bucket里面的数据
  bool flag = false;
  for (const std::pair<KeyType, ValueType> &kv : res) {
    auto the_key = kv.first;
    auto the_value = kv.second;
    auto get_idx = KeyToDirectoryIndex(the_key, dir_page);
    if (get_idx == bucket_idx) {
      bucket_page->Insert(the_key, the_value, comparator_);
    } else {
      split_bucket_page->Insert(the_key, the_value, comparator_);
    }
  }
  if (bucket_page->IsFull() || split_bucket_page->IsFull()) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
    buffer_pool_manager_->UnpinPage(split_bucket_page_id, true, nullptr);
    table_latch_.WUnlock();
    return SplitInsert(transaction, key, value);
  }
  auto get_idx = KeyToDirectoryIndex(key, dir_page);
  if (get_idx == bucket_idx) {
    flag = bucket_page->Insert(key, value, comparator_);
  } else {
    flag = split_bucket_page->Insert(key, value, comparator_);
  }
  uint32_t diff = 1 << dir_page->GetLocalDepth(bucket_idx);
  auto dir_page_size = dir_page->Size();
  for (uint32_t i = bucket_idx; i >= 0; i -= diff) {
    dir_page->SetBucketPageId(i, bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(bucket_idx));
    if (i < diff) {
      break;
    }
  }
  for (uint32_t i = bucket_idx + diff; i < dir_page_size; i += diff) {
    dir_page->SetBucketPageId(i, bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(bucket_idx));
  }
  for (uint32_t i = split_bucket_page_idx; i >= 0; i -= diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_page_idx));
    if (i < diff) {
      break;
    }
  }
  for (uint32_t i = split_bucket_page_idx + diff; i < dir_page_size; i += diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_page_idx));
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  buffer_pool_manager_->UnpinPage(split_bucket_page_id, true, nullptr);
  return flag;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  auto dir_idx = KeyToDirectoryIndex(key, dir_page);
  auto bucket_page_id = dir_page->GetBucketPageId(dir_idx);
  HASH_TABLE_BUCKET_TYPE *get_bucket_page = FetchBucketPage(bucket_page_id);
  auto bucket_page_split_id = dir_page->GetSplitImageIndex(dir_idx);
  bool ans = get_bucket_page->Remove(key, value, comparator_);
  if (get_bucket_page->IsEmpty() && dir_page->GetLocalDepth(dir_idx) != 0 &&
      dir_page->GetLocalDepth(dir_idx) == dir_page->GetLocalDepth(bucket_page_split_id)) {
    Merge(transaction, key, value);
  }
  if (dir_page->CanShrink()) {
    dir_page->Shrink();
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  table_latch_.WUnlock();
  return ans;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto dir_page = FetchDirectoryPage();
  auto bucket_idx = KeyToDirectoryIndex(key, dir_page);
  auto bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  auto bucket_page_split_idx = dir_page->GetSplitImageIndex(bucket_idx);
  auto split_bucket_page_id = dir_page->GetBucketPageId(bucket_page_split_idx);
  dir_page->SetBucketPageId(bucket_idx, split_bucket_page_id);
  uint32_t diff = 1 << dir_page->GetLocalDepth(bucket_idx);
  auto dir_page_size = dir_page->Size();
  dir_page->DecrLocalDepth(bucket_page_split_idx);
  for (uint32_t i = bucket_page_split_idx; i >= 0; i -= diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(bucket_page_split_idx));
    if (i < diff) {
      break;
    }
  }
  for (uint32_t i = bucket_page_split_idx + diff; i < dir_page_size; i += diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(bucket_page_split_idx));
  }
  for (uint32_t i = bucket_idx; i >= 0; i -= diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(bucket_page_split_idx));
    if (i < diff) {
      break;
    }
  }
  for (uint32_t i = bucket_idx + diff; i < dir_page_size; i += diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(bucket_page_split_idx));
  }
  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  buffer_pool_manager_->DeletePage(bucket_page_id);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  buffer_pool_manager_->UnpinPage(split_bucket_page_id, true, nullptr);
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
