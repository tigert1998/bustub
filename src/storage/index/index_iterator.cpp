/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(Page *page, int key_index, BufferPoolManager *buffer_pool_manager)
    : page_(page), key_index_(key_index), buffer_pool_manager_(buffer_pool_manager) {
  if (page_ == nullptr || key_index_ < 0 || buffer_pool_manager_ == nullptr) {
    SetAsEnd();
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (!is_end_) {
    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::IsEnd() { return is_end_; }

INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::SetAsEnd() {
  is_end_ = true;
  key_index_ = -1;
  if (page_ != nullptr && buffer_pool_manager_ != nullptr) {
    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
  }
  page_ = nullptr;
  buffer_pool_manager_ = nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page_->GetData());
  return leaf_page->GetItem(key_index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (is_end_) return *this;
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page_->GetData());

  if (key_index_ < leaf_page->GetSize() - 1) {
    key_index_++;
  } else {
    auto next_page_id = leaf_page->GetNextPageId();

    if (next_page_id == INVALID_PAGE_ID) {
      SetAsEnd();
      return *this;
    }

    Page *page = buffer_pool_manager_->FetchPage(next_page_id);
    if (page == nullptr) throw Exception(ExceptionType::OUT_OF_MEMORY, "INDEXITERATOR_TYPE::operator++ out of memory");
    page->RLatch();
    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
    page_ = page;

    key_index_ = 0;
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
