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

#include <list>
#include <mutex>  // NOLINT
#include <vector>
#include <unordered_map>
#include <iostream>
#include <memory>

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
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> to_pos;
  std::list<frame_id_t> lru_cache;
  size_t num_pages;
  std::mutex the_mutex;
  //手写一个双链表的形式实现LRU
  struct DLinkedNode{
    frame_id_t frame_id;
    std::shared_ptr<DLinkedNode> next;
    std::shared_ptr<DLinkedNode> prev;
    DLinkedNode(frame_id_t id) : frame_id(id) {
      next = std::make_shared<DLinkedNode>(nullptr);
      prev = std::make_shared<DLinkedNode>(nullptr);
    }
    DLinkedNode() : frame_id(-1) {
      next = std::make_shared<DLinkedNode>(nullptr);
      prev = std::make_shared<DLinkedNode>(nullptr);
    }
  };
  size_t sizes;
  size_t capacity;
  std::shared_ptr<DLinkedNode> head;
  std::shared_ptr<DLinkedNode> tail;
  // TODO(student): implement me!
};

}  // namespace bustub
