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

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // std::lock_guard<std::mutex> lock(the_mutex_);
  std::scoped_lock<std::mutex> lock(the_mutex_);
  if (lru_cache_.empty()) {
    return false;
  }
  frame_id_t value = lru_cache_.back();
  lru_cache_.pop_back();
  *frame_id = value;
  to_pos_.erase(value);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  // std::lock_guard<std::mutex> lock(the_mutex_);
  std::scoped_lock<std::mutex> lcok(the_mutex_);
  // 如果在这个值已经被victim了
  if (to_pos_.count(frame_id) == 0U || lru_cache_.empty()) {
    return;
  }
  auto &pos = to_pos_[frame_id];
  lru_cache_.erase(pos);
  to_pos_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  // std::lock_guard<std::mutex> lock(the_mutex_);
  std::scoped_lock<std::mutex> lock(the_mutex_);
  // 如果已经存在对应的frame_id
  if (to_pos_.count(frame_id) != 0U) {
    return;
  }
  
  if (Size() == num_pages_) {
    frame_id_t victim_frame_id = -1;
    if (!this->Victim(&victim_frame_id)) {
      return;
    }
  }
  
  lru_cache_.push_front(frame_id);
  to_pos_[frame_id] = lru_cache_.begin();
}

size_t LRUReplacer::Size() {
  // size_t lru_size = lru_cache_.size();
  return to_pos_.size();
}

}  // namespace bustub
