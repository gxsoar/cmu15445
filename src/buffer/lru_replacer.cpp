//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages(num_pages) {
  // head = std::make_unique<DLinkedNode>(DLinkedNode());
  head = std::make_shared<DLinkedNode>();
  tail = std::make_shared<DLinkedNode>();
  head->next = tail;
  tail->prev = head;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(the_mutex);
  if (lru_cache.empty()) {
    return false;
  }
  frame_id_t value = lru_cache.back();
  lru_cache.pop_back();
  *frame_id = value;
  LOG_INFO("the victim value : %d\n", value);
  to_pos.erase(value);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(the_mutex);
  //如果在这个值已经被victim了
  if (!to_pos.count(frame_id) || lru_cache.empty()) {
    return;
  }
  auto &pos = to_pos[frame_id];
  // to_pos.erase(frame_id);
  lru_cache.erase(pos);
  to_pos.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(the_mutex);
  //如果已经存在对应的frame_id
  if (to_pos.count(frame_id)) {
    return;
  }
  else {
    if (num_pages == lru_cache.size()) {
      lru_cache.pop_back();
    }
    lru_cache.push_front(frame_id);
    to_pos[frame_id] = lru_cache.begin();
  }
}

size_t LRUReplacer::Size() { 
  std::lock_guard<std::mutex> lock(the_mutex);
  size_t lru_size = lru_cache.size();
  LOG_INFO("the LRUReplacer size is : %lu\n", lru_size);
  return lru_size;
}

}  // namespace bustub
