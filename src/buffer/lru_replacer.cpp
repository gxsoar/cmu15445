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

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_(num_pages) {
  // head = std::make_unique<DLinkedNode>(DLinkedNode());
  // head_ = new DLinkedNode(-1);
  // tail_ = new DLinkedNode(-1);
  // head_->next_node_ = tail_;
  // tail_->pre_node_ = head_;
}

LRUReplacer::~LRUReplacer() {
  // // delete head_;
  // // delete tail_;
  // DLinkedNode* cur_node = head_;
  // DLinkedNode* nex_node;
  // for (size_t i = 0; i < capacity_ + 2; ++ i) {
  //   nex_node = cur_node->next_node_;
  //   delete cur_node;
  //   cur_node = nex_node;
  // }
}

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(the_mutex_);
  if (lru_cache_.empty()) {
    return false;
  }
  // if (capacity_ == 0) {
  //   return false;
  // }
  // auto node = tail_->pre_node_;
  // //执行删除操作
  // *frame_id = node->id_;
  // // cache_.erase(*frame_id);
  // node->next_node_->pre_node_ = node->pre_node_;
  // node->pre_node_->next_node_ = node->next_node_;
  // // auto pre_node = node->pre_node_;
  // // tail_->pre_node_ = pre_node;
  // // pre_node->next_node_ = tail_;
  // --capacity_;
  // delete node;
  // cache_.erase(*frame_id);

  frame_id_t value = lru_cache_.back();
  lru_cache_.pop_back();
  *frame_id = value;
  // LOG_INFO("the victim value : %d\n", *frame_id);
  to_pos_.erase(value);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(the_mutex_);

  // 如果在这个值已经被victim了
  if (!to_pos_.count(frame_id) || lru_cache_.empty()) {
    return;
  }

  auto &pos = to_pos_[frame_id];
  // to_pos.erase(frame_id);
  lru_cache_.erase(pos);
  to_pos_.erase(frame_id);

  // if (cache_.count(frame_id) != 0U || capacity_ == 0) {
  //   return;
  // }
  // auto &pos = cache_[frame_id];
  // // cache.erase(frame_id);
  // DLinkedNode *p = tail_->pre_node_;
  // while (p != head_ && p != pos) {
  //   p = p->pre_node_;
  // }
  // DLinkedNode *p_prev = p->pre_node_;
  // DLinkedNode *p_next = p->next_node_;
  // p_prev->next_node_ = p_next;
  // p_next->pre_node_ = p_prev;
  // delete p;
  // --capacity_;
  // cache_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(the_mutex_);

  // 如果已经存在对应的frame_id
  if (to_pos_.count(frame_id)) {
    return;
  }
  else {
    if (num_pages_ == lru_cache_.size()) {
      lru_cache_.pop_back();
    }
    lru_cache_.push_front(frame_id);
    to_pos_[frame_id] = lru_cache_.begin();
  }

  // if (cache_.count(frame_id) == 0U) {
  //   return;
  // } 
  // if (sizes_ == capacity_) {
  //   auto node = tail_->pre_node_;
  //   auto pre_node = node->pre_node_;
  //   pre_node->next_node_ = tail_;
  //   tail_->pre_node_ = pre_node;
  //   delete node;
  //   --capacity_;
  // }
  // DLinkedNode *insert_node = new DLinkedNode(frame_id);
  // insert_node->next_node_ = head_->next_node_;
  // head_->next_node_->pre_node_ = insert_node;
  // insert_node->pre_node_ = head_;
  // head_->next_node_ = insert_node;
  // cache_[frame_id] = insert_node;
  // ++capacity_;
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock(the_mutex_);
  size_t lru_size = lru_cache_.size();

  // size_t cache_size = capacity_;
  // LOG_INFO("the LRUReplacer size is : %lu\n", cache_size);

  return lru_size;

  // return cache_size;
}

}  // namespace bustub
