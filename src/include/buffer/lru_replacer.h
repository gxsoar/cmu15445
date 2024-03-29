//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> to_pos_;
  std::list<frame_id_t> lru_cache_;
  size_t num_pages_;
  std::mutex the_mutex_;

  // // 手写一个双链表的形式实现LRU

  // struct DLinkedNode {
  //   frame_id_t id_ = {-1};
  //   DLinkedNode *next_node_ = {nullptr};
  //   DLinkedNode *pre_node_ = {nullptr};
  //   explicit DLinkedNode(frame_id_t id) : id_(id), next_node_(nullptr), pre_node_(nullptr) {}
  //   // DLinkedNode() : id_(-1), next_node_(nullptr), pre_node_(nullptr) {}
  // };
  // std::unordered_map<frame_id_t, DLinkedNode *> cache_;
  // size_t sizes_;     // 用来记录LRU的最大容量
  // size_t capacity_;  // 用来记录lru的容量的大小
  // DLinkedNode *head_;
  // DLinkedNode *tail_;
  // TODO(student): implement me!
};

}  // namespace bustub
