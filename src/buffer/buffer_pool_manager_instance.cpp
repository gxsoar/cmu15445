//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

#include "common/logger.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  // std::lock_guard<std::mutex> lock(latch_);
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0U || page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  pages_[frame_id].is_dirty_ = false;
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  // std::lock_guard<std::mutex> lock(latch_);
  for (int i = 0; i < next_page_id_; ++i) {
    FlushPgImp(i);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  std::scoped_lock<std::mutex> lock(latch_);
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  size_t cnt = 0;
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].GetPinCount() != 0) {
      ++cnt;
    }
  }
  if (cnt == pool_size_) {
    return nullptr;
  }
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    bool ok = replacer_->Victim(&frame_id);
    if (!ok) {
      return nullptr;
    }
    if (frame_id == -1) {
      return nullptr;
    }
    if (pages_[frame_id].IsDirty()) {
      pages_[frame_id].is_dirty_ = false;
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }
  }
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  page_table_.erase(pages_[frame_id].GetPageId());
  *page_id = AllocatePage();
  page_table_[*page_id] = frame_id;
  Page *new_page = &pages_[frame_id];
  new_page->page_id_ = *page_id;
  new_page->pin_count_ = 1;
  new_page->is_dirty_ = false;
  new_page->ResetMemory();
  disk_manager_->WritePage(new_page->GetPageId(), new_page->GetData());
  // 4.   Set the page ID output parameter. Return a pointer to P.
  replacer_->Pin(frame_id);
  return new_page;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  if (page_table_.count(page_id) != 0U) {
    auto frame_id = page_table_[page_id];
    Page *the_page = &pages_[frame_id];
    the_page->pin_count_++;
    replacer_->Pin(frame_id);
    return the_page;
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    bool ok = replacer_->Victim(&frame_id);
    if (!ok) {
      return nullptr;
    }
  }
  if (frame_id == -1) {
    return nullptr;
  }
  // 2.     If R is dirty, write it back to the disk.
  // 写这个实验要想明白一件事，the_page的page_id和给定的page_id不是一回事
  Page *the_page = &pages_[frame_id];
  if (the_page->IsDirty()) {
    the_page->is_dirty_ = false;
    disk_manager_->WritePage(the_page->GetPageId(), the_page->GetData());
  }
  // 3.     Delete R from the page table and insert P.
  page_table_.erase(the_page->GetPageId());
  page_table_[page_id] = frame_id;
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  the_page->page_id_ = page_id;
  the_page->pin_count_ = 1;
  the_page->ResetMemory();
  replacer_->Pin(frame_id);
  disk_manager_->ReadPage(page_id, the_page->GetData());
  return the_page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // std::lock_guard<std::mutex> lock(latch_);
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  auto the_pos = page_table_.find(page_id);
  // 1.   If P does not exist, return true.
  if (the_pos == page_table_.end()) {
    return true;
  }
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  auto frame_id = page_table_[page_id];
  Page *the_page = &pages_[frame_id];
  if (the_page->GetPinCount() != 0) {
    return false;
  }
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  page_table_.erase(page_id);
  the_page->ResetMemory();
  the_page->page_id_ = INVALID_PAGE_ID;
  the_page->pin_count_ = 0;
  free_list_.push_back(frame_id);
  the_page->is_dirty_ = false;
  // disk_manager_->WritePage(the_page->GetPageId(), the_page->GetData());
  DeallocatePage(page_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // std::lock_guard<std::mutex> lock(latch_);
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  if (page_table_.count(page_id) == 0U) {
    return true;
  }
  auto frame_id = page_table_[page_id];
  Page *the_page = &pages_[frame_id];
  if (is_dirty) {
    the_page->is_dirty_ = true;
  }
  if (the_page->GetPinCount() < 0) {
    return false;
  }
  if (the_page->GetPinCount() > 0) {
    --the_page->pin_count_;
  }
  if (the_page->GetPinCount() == 0) {
    replacer_->Unpin(frame_id);
    // disk_manager_->WritePage(the_page->GetPageId(), the_page->GetData());
  }
  // delete the_page;
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
