//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = B_PLUS_TREE_LEAF_PAGE_TYPE;

 public:
  IndexIterator(Page *page, int key_index, BufferPoolManager *buffer_pool_manager);
  ~IndexIterator();

  bool IsEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &iter) const { return page_ == iter.page_ && key_index_ == iter.key_index_; }

  bool operator!=(const IndexIterator &iter) const { return !this->operator==(iter); }

 private:
  void SetAsEnd();

  Page *page_;
  int key_index_;
  BufferPoolManager *buffer_pool_manager_;
  bool is_end_;
};

}  // namespace bustub
