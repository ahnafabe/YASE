/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II, Spring 2025
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 */

#include <random>
#include <mutex>
// #include <shared_mutex>
#include "skiplist.h"
#include "stack"

namespace yase {
SkipList::SkipList(uint32_t key_size, uint32_t payload_size) {
  // TODO: Your implementation
  this->key_size = key_size;
  this->payload_size = payload_size;
  this->height = 1;
  head = SkipListNode(SKIP_LIST_MAX_LEVEL, key_size, payload_size);
  tail = SkipListNode(SKIP_LIST_MAX_LEVEL, key_size, payload_size);

  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; ++i) {
    head.next[i] = &tail;
    tail.next[i] = nullptr;
    pthread_rwlock_init(&latches[i], nullptr);
  }
}

SkipList::~SkipList() {
  // TODO: Your implementation
  SkipListNode* curr = head.next[0];
  while (curr != &tail) {
    SkipListNode* temp = curr->next[0];
    delete curr;
    curr = temp;
  }
  for (int i = 0; i < SKIP_LIST_MAX_LEVEL; ++i) {
    pthread_rwlock_destroy(&latches[i]);
  }
}

SkipListNode *SkipList::NewNode(uint32_t levels, const char *key, const char *payload) {
  // TODO: Your implementation
  if (levels > SKIP_LIST_MAX_LEVEL) return nullptr;
  size_t size = sizeof(SkipListNode) + key_size + payload_size;
  char* c = new char[size];
  auto newNode = new (c) SkipListNode(levels, key_size, payload_size);
  memcpy(newNode->GetKey(), key, key_size);
  memcpy(newNode->GetPayload(), payload, payload_size);
  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; ++i) {
    newNode->next[i] = nullptr;
  }
  return newNode;
}

bool SkipList::Insert(const char *key, const char *payload) {
  // TODO: Your implementation
  // random number generator
  uint32_t nodeLevel = 1;
  std::random_device rd;  // a seed source for the random number engine
  std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()
  std::uniform_int_distribution<> distrib(0, 1);
  while ((distrib(gen)) == 0 && nodeLevel < SKIP_LIST_MAX_LEVEL) {
    nodeLevel++;
  }
  SkipListNode* node[SKIP_LIST_MAX_LEVEL];
  SkipListNode* curr = &head;
  uint32_t local_height = height;

  // updates height
  if (nodeLevel > local_height) {
    for (uint32_t i = nodeLevel - 1; i >= local_height; i--) {
      pthread_rwlock_wrlock(&latches[i]);
      node[i] = &head;
    }
  }
  
  for (int i = local_height - 1; i >= 0; i--) {
    if ((uint32_t)i >= nodeLevel){
      pthread_rwlock_rdlock(&latches[i]);
    }
    else{
      pthread_rwlock_wrlock(&latches[i]); 
    }
    while (curr->next[i] != &tail && memcmp(curr->next[i]->GetKey(), key, key_size) < 0) {
      curr = curr->next[i];
    }
    node[i] = curr;
  }
  curr = curr->next[0];
  // printf("%d has %d locks, reached the bottom\n", *key, locks);
  // check if it exists
  if (curr != &tail && memcmp(curr->GetKey(), key, key_size) == 0) {
    for (uint32_t i = 0; i < local_height; i++) {
      pthread_rwlock_unlock(&latches[i]);
    }
    for (uint32_t i = local_height; i < nodeLevel; i++) {
      pthread_rwlock_unlock(&latches[i]);
    }
    return false;
  }

  SkipListNode* newNode = NewNode(nodeLevel, key, payload);
  if (!newNode){
    for (uint32_t i = 0; i < local_height; i++) {
      pthread_rwlock_unlock(&latches[i]);
    }
    for (uint32_t i = local_height; i < nodeLevel; i++) {
      pthread_rwlock_unlock(&latches[i]);
    }
    return false;
  }

  for (uint32_t i = 0; i < nodeLevel; i++) {
    newNode->next[i] = node[i]->next[i];
    node[i]->next[i] = newNode;
    if(i > height - 1) height ++;
    pthread_rwlock_unlock(&latches[i]);
  }
  if(local_height > nodeLevel){
    for (uint32_t i = nodeLevel; i < local_height; i++) {
      pthread_rwlock_unlock(&latches[i]);
    }
  }
  return true;
}

bool SkipList::Search(const char *key, char *out_payload) {
  // TODO: Your implementation
  SkipListNode* curr = &head;
  uint32_t local_height = height;
  pthread_rwlock_rdlock(&latches[local_height - 1]);
  for (int i = local_height - 1; i >= 0; i--) {
    while (curr->next[i] != &tail && memcmp(curr->next[i]->GetKey(), key, key_size) < 0) {
      curr = curr->next[i];
    }
    if (i > 0) {
      pthread_rwlock_rdlock(&latches[i - 1]);
    }
    if (i != 0) {
      pthread_rwlock_unlock(&latches[i]);
    }
  }

  curr = curr->next[0];

  if (curr != &tail && memcmp(curr->GetKey(), key, key_size) == 0) {
    if (out_payload) {
      memcpy(out_payload, curr->GetPayload(), curr->payload_size);
    }
    pthread_rwlock_unlock(&latches[0]);
    return true;
  }
  pthread_rwlock_unlock(&latches[0]);
  return false;
}

bool SkipList::Update(const char *key, const char *payload) {
  // TODO: Your implementation
  SkipListNode* curr = &head;
  uint32_t local_height = height;
  for (int i = local_height - 1; i >= 0; i--) {
    pthread_rwlock_rdlock(&latches[i]);
    while (curr->next[i] != &tail && memcmp(curr->next[i]->GetKey(), key, key_size) < 0) {
      curr = curr->next[i];
    }
    if(i != 0){
      pthread_rwlock_unlock(&latches[i]);
    }
  }
  curr = curr->next[0];
  if (curr != &tail && memcmp(curr->GetKey(), key, key_size) == 0) {
    memcpy(curr->GetPayload(), payload, payload_size);
    pthread_rwlock_unlock(&latches[0]);
    return true;
  }
  pthread_rwlock_unlock(&latches[0]);
  return false;
}

bool SkipList::Delete(const char *key) {
  // TODO: Your implementation
  SkipListNode* node[SKIP_LIST_MAX_LEVEL];
  SkipListNode* curr = &head;
  uint32_t local_height = height;
  
  for (int level = local_height - 1; level >= 0; --level) {
    pthread_rwlock_wrlock(&latches[level]);
  }

  for (int i = local_height - 1; i >= 0; i--) {
    while (curr->next[i] != &tail && memcmp(curr->next[i]->GetKey(), key, key_size) < 0) {
      curr = curr->next[i];
    }
    node[i] = curr;
  }
  curr = curr->next[0];

  if (curr == &tail || memcmp(curr->GetKey(), key, key_size) != 0) {
    for (uint32_t i = 0; i < local_height; ++i) {
      pthread_rwlock_unlock(&latches[i]);
    }
    return false;
  }

  for (uint32_t i = curr->nlevels; i < local_height; ++i) {
    pthread_rwlock_unlock(&latches[i]);
  }

  for (uint32_t i = 0; i < curr->nlevels; i++) {
    SkipListNode *prev = node[i]; 
    if (prev->next[i] == curr) {
      prev->next[i] = curr->next[i];
    }
    pthread_rwlock_unlock(&latches[i]);
  }
  
  delete curr;

  return true;
}

void SkipList::Scan(const char *start_key, uint32_t nkeys, bool inclusive,
                           std::vector<std::pair<char *, char *> > *out_records) {
  // TODO: Your implementation
  if(nkeys == 0 || !out_records) return;
  if(head.next[0] == &tail) return;
  
  SkipListNode* curr = &head;
  uint32_t local_height = height;

  //Determine the start
  for (int i = local_height - 1; i >= 0; i--) {
    pthread_rwlock_rdlock(&latches[i]);
    while (curr->next[i] != &tail && memcmp(curr->next[i]->GetKey(), start_key, key_size) < 0) {
      curr = curr->next[i];
    }
    if(i != 0){
      pthread_rwlock_unlock(&latches[i]);
    }
  }

  curr = (start_key == nullptr) ? head.next[0] : curr->next[0];
  if(curr == &tail){
    pthread_rwlock_unlock(&latches[0]);
    return;
  }
  if (!inclusive && start_key != nullptr && curr != &tail && memcmp(curr->GetKey(), start_key, curr->key_size) == 0) {
    curr = curr->next[0]; 
    if(curr == &tail){
      pthread_rwlock_unlock(&latches[0]);
      return;
    }
  }

  for (int i = local_height - 1; i >= 1; --i) pthread_rwlock_unlock(&latches[i]);

  uint32_t scanned = 0;
  while (curr != &tail && scanned < nkeys) {
    char *key_copy = (char *)malloc(key_size);
    char *payload_copy = (char *)malloc(payload_size);
    if (key_copy == nullptr || payload_copy == nullptr) {
      pthread_rwlock_unlock(&latches[0]);
      return;
    }
    memcpy(key_copy, curr->GetKey(), key_size);
    memcpy(payload_copy, curr->GetPayload(), payload_size);

    out_records->push_back({key_copy, payload_copy});
    curr = curr->next[0];
    ++scanned;
  }

  pthread_rwlock_unlock(&latches[0]); 
}

}  // namespace yase
