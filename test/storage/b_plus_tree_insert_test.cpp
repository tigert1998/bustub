/**
 * b_plus_tree_insert_test.cpp
 */

#include <algorithm>
#include <cstdio>
#include <memory>
#include <random>

#include "b_plus_tree_test_util.h"  // NOLINT
#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

TEST(BPlusTreeTests, InsertTest1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, InsertTest2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {5, 4, 3, 2, 1};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  start_key = 3;
  current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, ConcurrentInsertTest) {
  const int N_THREADS = 8;
  const int KEYS_PER_THREAD = 1 << 14;
  const int SEED = 10086;

  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50 * N_THREADS, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::thread threads[N_THREADS];

  for (int i = 0; i < N_THREADS; i++) {
    threads[i] = std::thread(
        [&](int id) {
          int base = id * KEYS_PER_THREAD;
          std::vector<int> keys(KEYS_PER_THREAD);
          for (int i = 0; i < KEYS_PER_THREAD; i++) keys[i] = base + i;
          std::shuffle(keys.begin(), keys.end(), std::default_random_engine(SEED + id));

          RID rid;
          GenericKey<8> index_key;
          // create transaction
          auto transaction = std::make_unique<Transaction>(0);

          for (int i = 0; i < KEYS_PER_THREAD; i++) {
            int key = keys[i];
            int64_t value = key & 0xFFFFFFFF;
            rid.Set(0, value);
            index_key.SetFromInteger(key);
            tree.Insert(index_key, rid, transaction.get());
          }
        },
        i);
  }

  for (int i = 0; i < N_THREADS; i++) threads[i].join();

  GenericKey<8> index_key;
  std::vector<RID> rids;
  for (int key = 0; key < N_THREADS * KEYS_PER_THREAD; key++) {
    rids.clear();
    index_key.SetFromInteger(key);
    EXPECT_TRUE(tree.GetValue(index_key, &rids));
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  for (int i = 0; i < N_THREADS; i++) {
    threads[i] = std::thread(
        [&](int id) {
          GenericKey<8> index_key;

          auto rd = std::default_random_engine(SEED + id);
          int64_t start_key = std::uniform_int_distribution<>(0, N_THREADS * KEYS_PER_THREAD - 1)(rd);
          int64_t current_key = start_key;
          index_key.SetFromInteger(start_key);
          for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
            auto location = (*iterator).second;
            EXPECT_EQ(location.GetPageId(), 0);
            int64_t value = current_key & 0xFFFFFFFF;
            EXPECT_EQ(location.GetSlotNum(), value);
            current_key = current_key + 1;
          }

          EXPECT_EQ(current_key, N_THREADS * KEYS_PER_THREAD);
        },
        i);
  }

  for (int i = 0; i < N_THREADS; i++) threads[i].join();

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}
}  // namespace bustub
