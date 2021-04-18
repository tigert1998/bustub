//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <type_traits>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
thread_local typename BPLUSTREE_TYPE::LatchRegistry BPLUSTREE_TYPE::latch_registry_{};

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LatchRecord::Latch() {
  if (is_write)
    page->WLatch();
  else
    page->RLatch();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LatchRecord::Unlatch() {
  if (is_write)
    page->WUnlatch();
  else
    page->RUnlatch();
}

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_.load() == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  if (IsEmpty()) return false;

  Page *page = FindLeafPage(key, false);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page);

  ValueType value;
  auto ret = leaf_page->Lookup(key, &value, comparator_);
  if (ret) {
    result->resize(1);
    result->operator[](0) = value;
  }

  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (IsEmpty() && StartNewTree(key, value)) {
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t page_id;
  if (Page *root = buffer_pool_manager_->NewPage(&page_id); root != nullptr) {
    root->WLatch();
    page_id_t expected = INVALID_PAGE_ID;
    if (!root_page_id_.compare_exchange_strong(expected, page_id)) {
      root->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, false);
      buffer_pool_manager_->DeletePage(page_id);
      return false;
    }
    UpdateRootPageId(true);

    LeafPage *tree_page = reinterpret_cast<LeafPage *>(root->GetData());
    tree_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    tree_page->Insert(key, value, comparator_);
    root->WUnlatch();
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  } else {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "BPLUSTREE_TYPE::StartNewTree out of memory");
  }
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page *page = InternalFindLeafPage(&key, false, LatchMode::UPDATE);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  auto unlatch = [&](bool is_dirty) {
    for (auto kv : this->latch_registry_) {
      auto latch_record = kv.second;
      latch_record.Unlatch();
      buffer_pool_manager_->UnpinPage(latch_record.page->GetPageId(), is_dirty);
    }
    this->latch_registry_.clear();
  };

  auto try_insert_in_leaf = [&]() -> int {
    int ret = -1;

    // try to insert if page is not full
    if (auto current_size = leaf_page->GetSize(); current_size < leaf_page->GetMaxSize()) {
      ret = leaf_page->Insert(key, value, comparator_) > current_size;
    }
    // key already exists
    else if (int idx = leaf_page->KeyIndex(key, this->comparator_);
             this->comparator_(key, leaf_page->KeyAt(idx)) == 0) {
      ret = 0;
    }

    if (ret >= 0) {
      unlatch(ret == 1);
    }

    return ret;
  };

  int ret = try_insert_in_leaf();
  if (ret >= 0) return ret;

  unlatch(false);
  page = InternalFindLeafPage(&key, false, LatchMode::INSERT);
  leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  ret = try_insert_in_leaf();
  if (ret >= 0) return ret;

  LeafPage *new_page = Split(leaf_page);
  InsertIntoParent(leaf_page, new_page->KeyAt(0), new_page);
  if (comparator_(key, new_page->KeyAt(0)) < 0) {
    leaf_page->Insert(key, value, comparator_);
  } else {
    new_page->Insert(key, value, comparator_);
  }

  reinterpret_cast<Page *>(new_page)->WUnlatch();
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);

  unlatch(true);

  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) throw Exception(ExceptionType::OUT_OF_MEMORY, "BPLUSTREE_TYPE::Split out of memory");
  page->WLatch();

  N *sibling = reinterpret_cast<N *>(page->GetData());
  sibling->Init(page_id, node->GetParentPageId(), node->GetMaxSize());

  if constexpr (std::is_same_v<N, LeafPage>) {
    node->MoveHalfTo(sibling);
    sibling->SetNextPageId(node->GetNextPageId());
    node->SetNextPageId(page_id);
  } else {
    node->MoveHalfTo(sibling, buffer_pool_manager_);
  }

  return sibling;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  auto parent_page_id = old_node->GetParentPageId();

  if (parent_page_id == INVALID_PAGE_ID) {
    page_id_t new_root_page_id;
    Page *page = buffer_pool_manager_->NewPage(&new_root_page_id);
    if (page == nullptr)
      throw Exception(ExceptionType::OUT_OF_MEMORY, "BPLUSTREE_TYPE::InsertIntoParent out of memory");

    page->WLatch();
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(page->GetData());
    internal_page->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    internal_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);

    root_page_id_.store(new_root_page_id);
    UpdateRootPageId(false);

    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    return;
  }

  Page *parent_page = latch_registry_[parent_page_id].page;
  // parent page must already be in the buffer pool and be write latched

  InternalPage *parent_tree_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
  if (parent_tree_page->GetSize() < parent_tree_page->GetMaxSize()) {
    parent_tree_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  } else {
    InternalPage *sibling_tree_page = Split(parent_tree_page);

    Page *sibling_page = reinterpret_cast<Page *>(sibling_tree_page);

    if (auto current_size = parent_tree_page->GetSize();
        parent_tree_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId()) == current_size) {
      sibling_tree_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      new_node->SetParentPageId(sibling_tree_page->GetPageId());
    }

    InsertIntoParent(parent_tree_page, sibling_tree_page->KeyAt(0), sibling_tree_page, transaction);

    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { return false; }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  Page *page = InternalFindLeafPage(nullptr, true, LatchMode::READ);
  return INDEXITERATOR_TYPE(page, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *page = FindLeafPage(key, false);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  auto index = leaf_page->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(page, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(nullptr, -1, nullptr); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if left_most flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool left_most) {
  return InternalFindLeafPage(&key, left_most, LatchMode::READ);
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::InternalFindLeafPage(const KeyType *key, bool left_most, LatchMode latch_mode) {
  if (IsEmpty()) return nullptr;

  auto next_page_id = root_page_id_.load();

  latch_registry_.clear();

  bool first_round = true;

  while (1) {
  retry_first_round:
    Page *page = buffer_pool_manager_->FetchPage(next_page_id);
    if (page == nullptr)
      throw Exception(ExceptionType::OUT_OF_MEMORY, "BPLUSTREE_TYPE::InternalFindLeafPage out of memory");
    BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

    LatchRecord latch_record;
    latch_record.page = page;
    if (tree_page->IsLeafPage()) {
      latch_record.is_write = latch_mode != LatchMode::READ;
    } else {
      latch_record.is_write = latch_mode != LatchMode::UPDATE && latch_mode != LatchMode::READ;
    }
    latch_record.Latch();

    if (first_round && next_page_id != root_page_id_.load()) {
      latch_record.Unlatch();
      buffer_pool_manager_->UnpinPage(next_page_id, false);
      next_page_id = root_page_id_.load();
      goto retry_first_round;
    }

    first_round = false;

    bool release_parents = false;
    if (latch_mode == LatchMode::READ || latch_mode == LatchMode::UPDATE) {
      release_parents = true;
    } else if (latch_mode == LatchMode::INSERT) {
      release_parents = tree_page->GetSize() < tree_page->GetMaxSize();
    } else if (latch_mode == LatchMode::DELETE) {
      release_parents = tree_page->GetSize() > tree_page->GetMinSize();
    }
    if (release_parents) {
      while (!latch_registry_.empty()) {
        LatchRecord parent_latch_record = latch_registry_.begin()->second;
        parent_latch_record.Unlatch();
        buffer_pool_manager_->UnpinPage(parent_latch_record.page->GetPageId(), false);
        latch_registry_.erase(parent_latch_record.page->GetPageId());
      }
    }

    latch_registry_[next_page_id] = latch_record;

    if (tree_page->IsLeafPage()) {
      return page;
    }

    InternalPage *internal_page = reinterpret_cast<InternalPage *>(page->GetData());
    if (left_most) {
      next_page_id = internal_page->ValueAt(0);
    } else {
      next_page_id = internal_page->Lookup(*key, comparator_);
    }
  }
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(bool insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_.load());
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_.load());
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
