//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include <iostream>
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

#define SHIFT 0x3
#define MASK 0x7

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; ++bucket_idx) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }
    if (!IsReadable(bucket_idx)) {
      continue;
    }
    KeyType the_key = KeyAt(bucket_idx);
    if (cmp(the_key, key) == 0) {
      ValueType the_value = ValueAt(bucket_idx);
      result->push_back(the_value);
    }
  }
  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::GetAllValue(std::vector<std::pair<KeyType, ValueType>> *result) {
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; ++bucket_idx) {
    if (!IsReadable(bucket_idx)) {
      continue;
    }
    KeyType the_key = KeyAt(bucket_idx);
    ValueType the_value = ValueAt(bucket_idx);
    result->emplace_back(std::make_pair(the_key, the_value));
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::ClearBucket() {
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; ++bucket_idx) {
    RemoveAt(bucket_idx);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  std::vector<uint32_t> vec;
  size_t bucket_idx = 0;
  for (; bucket_idx < BUCKET_ARRAY_SIZE; ++bucket_idx) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }
    if (IsReadable(bucket_idx)) {
      KeyType the_key = KeyAt(bucket_idx);
      ValueType the_value = ValueAt(bucket_idx);
      if (cmp(the_key, key) == 0 && the_value == value) {
        return false;
      }
    } else {
      vec.push_back(bucket_idx);
    }
  }
  size_t avail_idx;
  if (!vec.empty()) {
    avail_idx = vec[0];
  } else {
    if (bucket_idx >= BUCKET_ARRAY_SIZE) {
      return false;
    }
    avail_idx = bucket_idx;
    SetOccupied(avail_idx);
  }
  array_[avail_idx].first = key;
  array_[avail_idx].second = value;
  SetReadable(avail_idx);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; ++bucket_idx) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }
    if (!IsReadable(bucket_idx)) {
      continue;
    }
    KeyType the_key = KeyAt(bucket_idx);
    ValueType the_value = ValueAt(bucket_idx);
    if (cmp(the_key, key) == 0 && the_value == value) {
      if (!IsReadable(bucket_idx)) {
        return false;
      }
      RemoveAt(bucket_idx);
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  readable_[bucket_idx >> SHIFT] &= ~(1 << (bucket_idx & MASK));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  //  获取occupied的第bucket_idx位，并设置为i
  return occupied_[bucket_idx >> SHIFT] & (1 << (bucket_idx & MASK));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  occupied_[bucket_idx >> SHIFT] |= (1 << (bucket_idx & MASK));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  return readable_[bucket_idx >> SHIFT] & (1 << (bucket_idx & MASK));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  readable_[bucket_idx >> SHIFT] |= (1 << (bucket_idx & MASK));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; ++bucket_idx) {
    if (!IsOccupied(bucket_idx)) {
      return false;
    }
    if (!IsReadable(bucket_idx)) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t cnt = 0;
  for (uint32_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (IsReadable(bucket_idx)) {
      ++cnt;
    }
  }
  return cnt;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; ++bucket_idx) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }
    if (IsReadable(bucket_idx)) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
