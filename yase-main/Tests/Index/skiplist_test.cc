/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II, Spring 2025
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 *
 * Test cases for skip list.
 */

#include <thread>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <Index/skiplist.h>

namespace yase {

class SkipListTest : public ::testing::Test {
 protected:
  SkipList *slist;

  void SetUp() override {
    slist = nullptr;
  }

  void TearDown() override {
    if (slist) {
      delete slist;
    }
    slist = nullptr;
  }

  void NewSkipList(uint32_t key_size, uint32_t payload_size) {
    slist = new SkipList(key_size, payload_size);
  }
};

// Empty list with pointers properly set up
TEST_F(SkipListTest, Init) {
  NewSkipList(8, 8);

  ASSERT_EQ(slist->key_size, 8);
  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; ++i) {
    ASSERT_EQ(slist->head.next[i], &slist->tail);
    ASSERT_EQ(slist->tail.next[i], nullptr);
  }
  ASSERT_EQ(slist->height, 1);
}

TEST_F(SkipListTest, NewNodeTooHigh) {
  NewSkipList(8, 8);
  uint64_t value = 0xfeedbeef;
  std::string key("testkey");

  // Level greater than max, should fail
  SkipListNode *node = slist->NewNode(100, key.c_str(), (char *)&value);
  ASSERT_FALSE(node);
  free(node);
}

TEST_F(SkipListTest, NewNode) {
  NewSkipList(8, 8);
  uint64_t value = 0xfeedbeef;
  std::string key("testkey1");

  // This one should succeed
  SkipListNode *node = slist->NewNode(4, key.c_str(), (char *)&value);
  ASSERT_TRUE(node);

  // Check level and value
  ASSERT_EQ(node->nlevels, 4);
  ASSERT_EQ(*(uint64_t *)node->GetPayload(), value);

  // Check key
  int cmp = memcmp(key.c_str(), node->GetKey(), 8);
  ASSERT_EQ(cmp, 0);

  // Check next pointers
  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; ++i) {
    ASSERT_EQ(node->next[i], nullptr);
  }

  free(node);
}

// Insert one key
TEST_F(SkipListTest, SingleInsertSearch) {
  NewSkipList(8, 8);
  uint64_t value = 0xfeedbeef;
  std::string key("testkeyk");

  bool success = slist->Insert(key.c_str(), (char *)&value);
  ASSERT_TRUE(success);

  // Read it back
  uint64_t v = 0;
  bool found = slist->Search(key.c_str(), (char *)&v);
  ASSERT_TRUE(found);
  ASSERT_EQ(value, v);
}

// Insert an existed key
TEST_F(SkipListTest, InsertExisted) {
  NewSkipList(8, 8);
  uint64_t value = 0xfeedbeef;
  std::string key("testkeyk");

  bool success = slist->Insert(key.c_str(), (char *)&value);
  ASSERT_TRUE(success);

  // Insert again
  success = slist->Insert(key.c_str(), (char *)&value);
  ASSERT_FALSE(success);
}

TEST_F(SkipListTest, SearchNonExist) {
  NewSkipList(8, 8);
  std::string key("11111111");

  // Empty, should turn up nothing
  uint64_t v = 0;
  bool found = slist->Search(key.c_str(), (char *)&v);
  ASSERT_FALSE(found);

  // Insert than search for a different key
  uint64_t value = 0xfeedbeef;
  bool success = slist->Insert(key.c_str(), (char *)&value);
  ASSERT_TRUE(success);

  std::string search_key("11111112");
  found = slist->Search(search_key.c_str(), (char *)&value);
  ASSERT_FALSE(found);
}

TEST_F(SkipListTest, Update) {
  NewSkipList(8, 8);
  std::string key("11111111");

  // Empty, should fail
  uint64_t value1 = 1;
  bool success = slist->Update(key.c_str(), (char *)&value1);
  ASSERT_FALSE(success);

  // Insert
  success = slist->Insert(key.c_str(), (char *)&value1);
  ASSERT_TRUE(success);
  uint64_t value11 = 0;
  bool found = slist->Search(key.c_str(), (char *)&value11);
  ASSERT_TRUE(found);
  ASSERT_EQ(value1, value11);

  // Now update, should succeed
  uint64_t value2 = 2;
  success = slist->Update(key.c_str(), (char *)&value2);
  ASSERT_TRUE(success);

  // Search should return value2
  uint64_t value22;
  found = slist->Search(key.c_str(), (char *)&value22);
  ASSERT_EQ(value2, value22);
}

// Check all nodes are sorted
TEST_F(SkipListTest, SortedList) {
  NewSkipList(8, 8);
  static const uint64_t kKeys = 1024;

  for (uint64_t k = 1; k <= kKeys; ++k) {
    bool success = slist->Insert((const char *)&k, (char *)&k);
    ASSERT_TRUE(success);
  }

  SkipListNode *curr = &slist->head;
  ASSERT_NE(curr->next[0], nullptr);
  ASSERT_NE(curr->next[0], &slist->tail);

  uint64_t nkeys = 0;
  curr = curr->next[0];
  uint64_t prev_key = ~uint64_t{0};
  while (curr != &slist->tail) {
    if (prev_key == ~uint64_t{0}) {
      ASSERT_EQ(nkeys, 0);
    } else {
      int cmp = memcmp(&prev_key, curr->GetKey(), 8);
      ASSERT_LE(cmp, 0);
    }
    prev_key = *(uint64_t*)&curr->GetKey()[0];
    ASSERT_GE(curr->nlevels, 1);
    curr = curr->next[0];
    ++nkeys;
  }
  ASSERT_EQ(&slist->tail, curr);
  ASSERT_EQ(slist->tail.next[0], nullptr);
  ASSERT_EQ(nkeys, kKeys);
}

TEST_F(SkipListTest, InsertSearchDelete) {
  NewSkipList(8, 8);

  // Insert 100 keys
  static const uint64_t kKeys = 100;
  for (uint64_t k = 1; k <= kKeys; ++k) {
    uint64_t value = k * 2;
    bool success = slist->Insert((const char *)&k, (char *)&value);
    ASSERT_TRUE(success);
  }
  LOG(INFO) << "Insert succeeded";

  // Read the inserted keys back
  for (uint64_t k = 1; k <= kKeys; ++k) {
    uint64_t r = 0;
    bool found = slist->Search((const char *)&k, (char *)&r);
    ASSERT_TRUE(found);
    ASSERT_EQ(r, k * 2);
  }

  LOG(INFO) << "Read succeeded";

  // Delete some inserted keys
  for (uint64_t k = 0; k < kKeys; ++k) {
    if (k % 2) {
      bool success = slist->Delete((const char *)&k);
      ASSERT_TRUE(success);
      // Read should fail now
      uint64_t r = 0;
      bool found = slist->Search((char *)&k, (char *)&r);
      ASSERT_FALSE(found);
    }
  }
}

TEST_F(SkipListTest, ForwardScanInclusive) {
  NewSkipList(8, 8);

  static const uint64_t kKeys = 200;
  uint64_t start_key = 1;
  for (uint64_t i = 1; i <= kKeys; ++i) {
    bool success = slist->Insert((const char *)&i, (char *)&i);
    ASSERT_TRUE(success);
  }

  std::vector<std::pair<char *, char *> > result;
  static const uint32_t nkeys = 10;
  slist->Scan((const char *)&start_key, nkeys, true, &result);
  ASSERT_EQ(result.size(), nkeys);

  for (uint32_t i = 1; i < result.size(); ++i) {
    auto &prev = result[i - 1];
    auto &r = result[i];
    int cmp = memcmp(prev.first, r.first, slist->key_size);
    ASSERT_LT(cmp, 0);
  }

  // Recycle memory allocated for keys
  for (auto &r : result) {
    free(r.first);
    free(r.second);
  }
}

TEST_F(SkipListTest, ForwardScanNonInclusive) {
  NewSkipList(8, 8);

  static const uint64_t kKeys = 6;
  for (uint64_t i = 1; i <= kKeys; ++i) {
    bool success = slist->Insert((const char *)&i, (char *)&i);
    ASSERT_TRUE(success);
  }

  std::vector<std::pair<char *, char *> > result;
  static const uint32_t nkeys = 30;

  // Non-existant, small start_key, scanning more than the list has: 
  // should return all keys
  uint64_t start_key = 0;
  slist->Scan((const char *)&start_key, nkeys, true, &result);
  ASSERT_EQ(result.size(), kKeys);

  for (uint32_t i = 1; i < result.size(); ++i) {
    auto &prev = result[i - 1];
    auto &r = result[i];
    int cmp = memcmp(prev.first, r.first, slist->key_size);
    ASSERT_LT(cmp, 0);
  }

  // Recycle memory allocated for keys
  for (auto &r : result) {
    free(r.first);
    free(r.second);
  }
}

void InsertSearch(uint32_t thread_id, yase::SkipList *slist) {
  static const uint64_t kKeys = 100;
  for (uint64_t k = 0; k < kKeys; ++k) {
    uint64_t key = k;
    key = k * 4 + thread_id;
    bool success = slist->Insert((char *)&key, (char *)&key);
    ASSERT_TRUE(success);

    // Issue a search for non-existant key
    key = 400 + k;
    uint64_t value = 0;
    bool found = slist->Search((char *)&key, (char *)&value);
    ASSERT_FALSE(found);
  }
}

// New tests with four threads inserting non-overlapping keys
TEST_F(SkipListTest, ConcurrentInsertSearch) {
  // Initialize a skip list that supports 8-byte key
  NewSkipList(8, 8);

  static const uint32_t kThreads = 4;
  std::vector<std::thread *> threads;
  for (uint32_t i = 0; i < kThreads; ++i) {
    threads.push_back(new std::thread(InsertSearch, i, slist));
  }

  // Wait for all threads to finish
  for (auto &t : threads) {
    t->join();
    delete t;
  }
}

// Long keys
TEST_F(SkipListTest, LongKeys) {
  // 20-byte keys
  NewSkipList(20, 8);

  auto insert = [&](uint32_t thread_id, SkipList *slist) {
    static const uint64_t kKeys = 100;
    for (uint64_t k = 0; k < kKeys; ++k) {
      char key[20];
      // Use the high-order bits for thread id
      *(uint32_t*)&key[16] = thread_id;
      // Remaining as a counter
      *(uint64_t*)&key[8] = k;
      *(uint64_t*)&key[0] = k;
      bool success = slist->Insert(key, (char *)&k);
      ASSERT_TRUE(success);
    }
  };

  static const uint32_t kThreads = 4;
  std::vector<std::thread *> threads;
  for (uint32_t i = 0; i < kThreads; ++i) {
    threads.push_back(new std::thread(insert, i, slist));
  }

  // Wait for all threads to finish
  for (auto &t : threads) {
    t->join();
    delete t;
  }

  // Read all keys back
  for (uint32_t i = 0; i < kThreads; ++i) {
    static const uint64_t kKeys = 100;
    for (uint64_t k = 0; k < kKeys; ++k) {
      char key[20];
      // Use the high-order bits for thread id
      *(uint32_t*)&key[16] = i;
      // Remaining as a counter
      *(uint64_t*)&key[8] = k;
      *(uint64_t*)&key[0] = k;
      uint64_t value = 0;
      bool found = slist->Search(key, (char *)&value);
      ASSERT_TRUE(found);
      ASSERT_EQ(value, k);
    }
  };
}

}  // namespace yase

int main(int argc, char **argv) {
  ::google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
