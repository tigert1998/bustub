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

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::unique_lock lock(mtx_);
  if (potential_victims_.empty()) {
    return false;
  }
  *frame_id = potential_victims_.back();
  potential_victims_.pop_back();
  id_to_iter_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::unique_lock lock(mtx_);
  if (id_to_iter_.count(frame_id) == 0) {
    return;
  }
  auto iter = id_to_iter_[frame_id];
  potential_victims_.erase(iter);
  id_to_iter_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::unique_lock lock(mtx_);
  if (id_to_iter_.count(frame_id) >= 1) {
    return;
  }
  potential_victims_.push_front(frame_id);
  id_to_iter_[frame_id] = potential_victims_.begin();
}

size_t LRUReplacer::Size() {
  std::shared_lock lock(mtx_);
  return id_to_iter_.size();
}

}  // namespace bustub
