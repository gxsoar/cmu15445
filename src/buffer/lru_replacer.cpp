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

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages(num_pages), sizes(num_pages) {
  // head = std::make_unique<DLinkedNode>(DLinkedNode());
  head = new DLinkedNode();
  tail = new DLinkedNode();
  head->next = tail;
  tail->prev = head;
}

LRUReplacer::~LRUReplacer() {
  delete head;
  delete tail;
}

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(the_mutex);
  // if (lru_cache.empty()) {
  //   return false;
  // }
  if (capacity == 0) {
    return false;
  }
  auto node = tail->prev;
  //执行删除操作
  *frame_id = node->frame_id;
  auto pre_node = node->prev;
  tail->prev = pre_node;
  pre_node->next = tail;
  delete node;
  --capacity;
  cache.erase(*frame_id);
  
  // frame_id_t value = lru_cache.back();
  // lru_cache.pop_back();
  // *frame_id = value;
  LOG_INFO("the victim value : %d\n", *frame_id);
  // to_pos.erase(value);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(the_mutex);
  //如果在这个值已经被victim了
  // // if (!to_pos.count(frame_id) || lru_cache.empty()) {
  // //   return;
  // // }
  
  // auto &pos = to_pos[frame_id];
  // // to_pos.erase(frame_id);
  // lru_cache.erase(pos);
  // to_pos.erase(frame_id);
  if (!cache.count(frame_id) || capacity == 0) {
    return;
  }
  auto &pos = cache[frame_id];
  // cache.erase(frame_id);
  DLinkedNode* p = tail;
  while(p != head && p != pos) {
    p = p->prev;
  }
  DLinkedNode* p_prev = p->prev;
  DLinkedNode* p_next = p->next;
  p_prev->next = p_next;
  p_next->prev = p_prev;
  delete p;
  --capacity;
  cache.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(the_mutex);
  //如果已经存在对应的frame_id
  // if (to_pos.count(frame_id)) {
  //   return;
  // }
  // else {
  //   if (num_pages == lru_cache.size()) {
  //     lru_cache.pop_back();
  //   }
  //   lru_cache.push_front(frame_id);
  //   to_pos[frame_id] = lru_cache.begin();
  // }
  if (cache.count(frame_id)) {
    return;
  }
  else {
    if (sizes == capacity) {
      auto node = tail->prev;
      auto pre_node = node->prev;
      pre_node->next = tail;
      tail->prev = pre_node;
      delete node;
      --capacity;
    }
    DLinkedNode* insert_node = new DLinkedNode(frame_id);
    insert_node->next = head->next;
    head->next->prev = insert_node;
    insert_node->prev = head;
    head->next = insert_node;
    cache[frame_id] = insert_node;
    ++capacity;
  }
}

size_t LRUReplacer::Size() { 
  std::lock_guard<std::mutex> lock(the_mutex);
  // size_t lru_size = lru_cache.size();
  size_t cache_size = capacity;
  LOG_INFO("the LRUReplacer size is : %lu\n", cache_size);
  // return lru_size;
  return cache_size;
}

}  // namespace bustub
