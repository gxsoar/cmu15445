//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  num_instance_ = num_instances;
  parallel_buffer_pool_sizes_ = num_instance_ * pool_size;
  pool_size_ = pool_size;
  start_index_ = 0;
  buffer_pool_.resize(num_instance_);
  for (size_t i = 0; i < num_instances; ++i) {
    // buffer_pool_[i]
    buffer_pool_[i] = new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (size_t i = 0; i < num_instance_; ++i) {
    delete buffer_pool_[i];
  }
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  // std::lock_guard<std::mutex> lock(latch_);
  return parallel_buffer_pool_sizes_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return buffer_pool_[page_id % num_instance_];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  auto buffer_pool_manager = GetBufferPoolManager(page_id);
  return buffer_pool_manager->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  return buffer_pool_[page_id % num_instance_]->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  return buffer_pool_[page_id % num_instance_]->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  std::lock_guard<std::mutex> lock(latch_);
  for (size_t i = 0; i < num_instance_; ++i) {
    auto page = buffer_pool_[start_index_]->NewPage(page_id);
    if (page != nullptr) {
      return page;
    }
    start_index_ = (start_index_ + 1) % num_instance_;
  }
  return nullptr;
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  return buffer_pool_[page_id % num_instance_]->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (size_t i = 0; i < num_instance_; ++i) {
    buffer_pool_[i]->FlushAllPages();
  }
}

}  // namespace bustub
