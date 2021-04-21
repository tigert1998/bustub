/**
 * b_plus_tree_delete_test.cpp
 */

#include <algorithm>
#include <cstdio>
#include <random>
#include <unordered_map>

#include "b_plus_tree_test_util.h"  // NOLINT
#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

TEST(BPlusTreeTests, DeleteTest1) {
  // create KeyComparator and index schema
  std::string createStmt = "a bigint";
  Schema *key_schema = ParseCreateStatement(createStmt);
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

  std::vector<int64_t> remove_keys = {1, 5};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  start_key = 2;
  current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 3);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, DeleteTest2) {
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

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  start_key = 2;
  current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, ConcurrentMixTest) {
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

  std::vector<int> keys(KEYS_PER_THREAD * N_THREADS);
  std::vector<int> delete_order(KEYS_PER_THREAD * N_THREADS);
  for (int i = 0; i < KEYS_PER_THREAD * N_THREADS; i++) keys[i] = i;

  auto engine = std::default_random_engine(SEED);
  std::shuffle(keys.begin(), keys.end(), engine);
  std::unordered_set<int> deleted_keys;

  for (int i = 0; i < KEYS_PER_THREAD * N_THREADS; i++) {
    int base = i / KEYS_PER_THREAD * KEYS_PER_THREAD;
    if (std::uniform_real_distribution<>(0, 1)(engine) <= 0.8) {
      auto idx = std::uniform_int_distribution<>(base, i)(engine);
      while (deleted_keys.count(keys[idx])) {
        idx = std::uniform_int_distribution<>(base, i)(engine);
      }
      delete_order[i] = idx;
      deleted_keys.insert(keys[idx]);
    } else {
      delete_order[i] = -1;
    }
  }

  for (int i = 0; i < N_THREADS; i++) {
    threads[i] = std::thread(
        [&](int id) {
          int base = id * KEYS_PER_THREAD;

          RID rid;
          GenericKey<8> index_key;
          // create transaction
          auto transaction = std::make_unique<Transaction>(0);

          for (int i = base; i < base + KEYS_PER_THREAD; i++) {
            int key = keys[i];
            int64_t value = key & 0xFFFFFFFF;
            rid.Set(0, value);
            index_key.SetFromInteger(key);
            tree.Insert(index_key, rid, transaction.get());

            if (delete_order[i] >= 0) {
              key = keys[delete_order[i]];
              index_key.SetFromInteger(key);
              tree.Remove(index_key, transaction.get());
            }
          }
        },
        i);
  }

  for (int i = 0; i < N_THREADS; i++) threads[i].join();

  keys.clear();
  keys.reserve(N_THREADS * KEYS_PER_THREAD);

  GenericKey<8> index_key;
  std::vector<RID> rids;
  for (int key = 0; key < N_THREADS * KEYS_PER_THREAD; key++) {
    rids.clear();
    index_key.SetFromInteger(key);

    if (deleted_keys.count(key)) {
      EXPECT_FALSE(tree.GetValue(index_key, &rids));
    } else {
      keys.push_back(key);
      EXPECT_TRUE(tree.GetValue(index_key, &rids));
      EXPECT_EQ(rids.size(), 1);
      if (rids.size() > 0) {
        int64_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].GetSlotNum(), value);
      }
    }
  }

  for (int i = 0; i < N_THREADS; i++) {
    threads[i] = std::thread(
        [&](int id) {
          GenericKey<8> index_key;

          auto rd = std::default_random_engine(SEED + id);
          int64_t start_key = std::uniform_int_distribution<>(0, N_THREADS * KEYS_PER_THREAD - 1)(rd);

          int idx = 0;
          while (keys[idx] < start_key) idx++;

          index_key.SetFromInteger(start_key);
          for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
            auto location = (*iterator).second;
            EXPECT_EQ(location.GetPageId(), 0);
            int64_t value = keys[idx] & 0xFFFFFFFF;
            EXPECT_EQ(location.GetSlotNum(), value);
            idx += 1;
          }

          EXPECT_EQ(idx, keys.size());
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
