//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
      dir_.emplace_back(std::make_shared<Bucket>(bucket_size));
    }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t idx=IndexOf(key);
  auto target_bucket=dir_[idx];
  return target_bucket->Find(key,value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  size_t idx=IndexOf(key);
  auto target_bucket=dir_[idx];
  return target_bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> guard(latch_);
  int idx=IndexOf(key);
  auto target_bucket=dir_[idx];
 
  //if insert an old one,no need to spilt,just update
  //update the old one   todo
  for (auto &item:target_bucket->GetItems()){
    if(key==item.first) {
      item.second=value;
      return;
    }
  }
 //bucket is full
  while (target_bucket->IsFull())
  {
    if(target_bucket->GetDepth()==GetGlobalDepthInternal()){
      //need resize dir_
      size_t capacity=dir_.size();
      dir_.resize(capacity<<1);
      global_depth_++;
      //allocate new dir entry :
      for (size_t i = 0; i < capacity; i++)
      {
        dir_[i+capacity]=dir_[i];
      }     
    }
    // bucket spilt:
    num_buckets_++;

    int mask=1<<target_bucket->GetDepth();
    auto zero_bucket=std::make_shared<Bucket> (bucket_size_,target_bucket->GetDepth()+1);
    auto one_bucket=std::make_shared<Bucket> (bucket_size_,target_bucket->GetDepth()+1);
    for(auto & item:target_bucket->GetItems()){
      size_t hashkey=std::hash<K>()(item.first);
      if((hashkey & mask) !=0U) one_bucket->Insert(item.first,item.second);
      else zero_bucket->Insert(item.first,item.second);
    }
    //if(!one_bucket->GetItems().empty()&&!zero_bucket->GetItems().empty()) 
    for (size_t i = 0; i < dir_.size(); i++)
    {
      if (dir_[i]==target_bucket)
      {
        if((i&mask)!=0U) dir_[i]=one_bucket;
        else dir_[i]=zero_bucket;
      }  
    }

    target_bucket=dir_[IndexOf(key)];
  }

  //need insert a new one
  target_bucket->Insert(key,value);

}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for(auto &item:GetItems()){
    if(item.first==key){
      value=item.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  V value;
  bool res=Find(key,value);
  if(res) list_.remove(std::pair<K,V>(key,value));
  return res;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  list_.emplace_back(std::pair<K,V> (key,value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
