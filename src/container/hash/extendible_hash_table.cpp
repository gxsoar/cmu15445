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
  directory_page_id_ = INVALID_PAGE_ID;
  HashTableDirectoryPage *dir_page_ = reinterpret_cast<HashTableDirectoryPage*>(buffer_pool_manager->NewPage(&directory_page_id_, nullptr));
  dir_page_->SetPageId(directory_page_id_);
  auto bucket_page_idx = INVALID_PAGE_ID;
  HashTableBucketPage<KeyType, ValueType,KeyComparator> *bucket_page_ = reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator>*>(buffer_pool_manager->NewPage(&bucket_page_idx, nullptr));
  dir_page_->SetBucketPageId(0,bucket_page_idx);
  buffer_pool_manager->UnpinPage(directory_page_id_, false, nullptr);
  buffer_pool_manager->UnpinPage(bucket_page_, false, nullptr);
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
  auto hash_value = Hash(key);
  auto global_mask = dir_page->GetGlobalDepthMask();
  return hash_value & global_mask;
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
  HashTableDirectoryPage *fetch_dir_page = reinterpret_cast<HashTableDirectoryPage*>(buffer_pool_manager_->FetchPage(directory_page_id_));
  return fetch_dir_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  HASH_TABLE_BUCKET_TYPE *bucket_page = reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator>*>(buffer_pool_manager_->FetchPage(bucket_page_id));
  return bucket_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  auto dir_idx = KeyToDirectoryIndex(key, dir_page_);
  auto bucket_page_id = dir_page_->GetBucketPageId(dir_idx);
  HASH_TABLE_BUCKET_TYPE *get_bucket_page = FetchBucketPage(bucket_page_id);
  return get_bucket_page->GetValue(key, comparator_, result);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto dir_page = FetchDirectoryPage();
  auto dir_idx = KeyToDirectoryIndex(key, dir_page);
  auto bucket_page_idx = dir_page->GetBucketPageId(dir_idx);
  HASH_TABLE_BUCKET_TYPE *get_bucket_page = FetchBucketPage(bucket_page_idx);
  bool ans = false;
  if (get_bucket_page->IsFull()) {
    ans = SplitInsert(transaction, key, value);
  } else {
    ans = get_bucket_page->Insert(key, value, comparator_);
  }
  buffer_pool_manager_->UnpinPage(dir_idx, true,  nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_idx, true, nullptr);
  return ans;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto dir_page = FetchDirectoryPage();
  auto dir_idx = KeyToDirectoryIndex(key, dir_page);
  std::vector<std::pair<KeyType, ValueType>> res;
  auto bucket_page_idx = dir_page->GetBucketPageId(dir_idx);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_idx);
  bucket_page->GetAllValue(&res);
  bucket_page->ClearBucket();
  auto split_bucket_page_idx = dir_page->GetSplitImageIndex(bucket_page_idx);
  auto split_bucket_page_id = INVALID_PAGE_ID;
  HASH_TABLE_BUCKET_TYPE *split_bucket_page = 
    reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator>*>
                        (buffer_pool_manager_->NewPage(split_bucket_page_id, nullptr));
  dir_page->IncrLocalDepth(bucket_page_idx);
  if (dir_page->GetLocalDepth(bucket_page_idx) == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
  }
  dir_page->SetBucketPageId(split_bucket_page_idx, split_bucket_page_id);
  //  重新分配原来bucket里面的数据
  bool flag = false;
  for (const std::pair<KeyType, ValueType> &kv : res) {
    assert(!Insert(transaction, kv.first, kv.second));
  }
  //  最后插入需要插入的kv
  flag = Insert(transaction, key, value);
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto dir_page = FetchDirectoryPage();
  auto dir_idx = KeyToDirectoryIndex(key, dir_page);
  auto bucket_page_idx = dir_page->GetBucketPageId(dir_idx);
  HASH_TABLE_BUCKET_TYPE *get_bucket_page = FetchBucketPage(bucket_page_idx);
  bool ans = get_bucket_page->Remove(key, value, comparator_);
  if (get_bucket_page->IsEmpty()) {
    Merge(transaction, key, value);
  }
  buffer_pool_manager_->UnpinPage(dir_idx, true, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_idx, true, nullptr);
  return ans;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {}

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
