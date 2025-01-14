//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::unique_lock<std::mutex> guard(latch_);
  if (page_table_.count(page_id) >= 1) {
    auto frame_id = page_table_[page_id];
    if (pages_[frame_id].pin_count_ == 0) {
      replacer_->Pin(frame_id);
    }
    pages_[frame_id].pin_count_++;
    return pages_ + frame_id;
  }

  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Victim(&frame_id)) {
    page_table_.erase(pages_[frame_id].GetPageId());

    // no need to acquire the wlatch of this page since no thread pins it
    InternalFlushPage(frame_id, false);
  } else {
    return nullptr;
  }

  page_table_[page_id] = frame_id;

  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

  return pages_ + frame_id;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::unique_lock<std::mutex> guard(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  auto frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ <= 0) {
    return true;
  }

  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }

  pages_[frame_id].is_dirty_ |= is_dirty;
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  std::unique_lock<std::mutex> guard(latch_);
  if (page_table_.count(page_id) >= 1) {
    auto frame_id = page_table_[page_id];
    pages_[frame_id].RLatch();
    InternalFlushPage(frame_id, true);
    pages_[frame_id].RUnlatch();
    return true;
  }
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::unique_lock<std::mutex> guard(latch_);
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Victim(&frame_id)) {
    page_table_.erase(pages_[frame_id].GetPageId());
    InternalFlushPage(frame_id, false);
  } else {
    return nullptr;
  }

  *page_id = disk_manager_->AllocatePage();
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].ResetMemory();

  page_table_[*page_id] = frame_id;

  return pages_ + frame_id;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  std::unique_lock<std::mutex> guard(latch_);
  if (page_table_.count(page_id) >= 1) {
    auto frame_id = page_table_[page_id];
    if (pages_[frame_id].pin_count_ >= 1) {
      return false;
    }
    page_table_.erase(page_id);
    free_list_.push_back(frame_id);
  }

  disk_manager_->DeallocatePage(page_id);

  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  std::unique_lock<std::mutex> guard(latch_);
  for (auto kv : page_table_) {
    auto frame_id = kv.second;
    pages_[frame_id].RLatch();
    InternalFlushPage(frame_id, false);
    pages_[frame_id].RUnlatch();
  }
}

void BufferPoolManager::InternalFlushPage(frame_id_t frame_id, bool force) {
  if (pages_[frame_id].IsDirty() || force) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
  }
}

}  // namespace bustub
