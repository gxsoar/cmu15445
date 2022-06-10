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

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(the_mutex);
  if (lru_cache.empty()) {
    return false;
  }
  auto pos = to_pos[*frame_id];
  lru_cache.erase(pos);
  to_pos.erase(*frame_id);
  // frame_id_t output_parameter = *frame_id;
  // std::cout << output_parameter << std::endl;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(the_mutex);
  Victim(&frame_id);
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
  return lru_size;
}

}  // namespace bustub
