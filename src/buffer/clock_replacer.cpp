//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages)
    : pined_(num_pages), tested_(num_pages), ptr_(0), size_(0), num_pages_(num_pages) {
  for (size_t i = 0; i < num_pages_; i++) {
    pined_[i] = true;
  }
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  if (size_ == 0) {
    return false;
  }
  while (true) {
    while (pined_[ptr_]) {
      ptr_ = (ptr_ + 1) % num_pages_;
    }
    if (tested_[ptr_]) {
      pined_[ptr_] = true;
      size_--;
      *frame_id = ptr_;
      return true;
    }
    tested_[ptr_] = true;
    ptr_ = (ptr_ + 1) % num_pages_;
  }
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  if (pined_[frame_id]) {
    return;
  }
  pined_[frame_id] = true;
  size_--;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  if (!pined_[frame_id]) {
    return;
  }
  pined_[frame_id] = false;
  tested_[frame_id] = false;
  size_++;
}

size_t ClockReplacer::Size() { return size_; }

}  // namespace bustub
