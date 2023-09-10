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
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
      dir_.push_back(std::make_shared<Bucket>(bucket_size));
    }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
 std::scoped_lock<std::mutex> lock(latch_);
 //bucket index
 auto index = IndexOf(key);
 //find in list of each bucket
 return dir_[index]->Find(key,value);
 }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  //UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);
  return dir_[index]->Remove(key);

}
//helper function for insert , bucket grow
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket)->void {
  bucket->IncrementDepth();
  int localdepth  = bucket->GetDepth();
  num_buckets_++;
  // new a bucket 
  std::shared_ptr<Bucket> p(new Bucket(bucket_size_,localdepth));
  int mask = (1 << localdepth) -1;
  int pre_mask = (1 << (localdepth-1))-1;
  auto pre_index = std::hash<K>() (( *(bucket->GetItems().begin()) ) .first) & pre_mask;
  for(auto it = bucket->GetItems().begin(); it != bucket->GetItems().end();){
    auto index = std::hash<K>()  (it->first) & mask;
    //
    if(index != pre_index){
      p->Insert(it->first, it->second);
      bucket->GetItems().erase(it++);
      dir_[index] = p;
    }
    else{ 
      ++it;
    }
  }
  // for(size_t i =0 ; i<dir_.size(); i++){
  //   if((i& pre_mask)==pre_index && ((i& mask) !=pre_index)){
  //     dir_[i] = p;
  //   }
  // }

}
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  //UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  while(true){
    auto index = IndexOf(key);
    bool flag = dir_[index]->Insert(key,value);
    if(flag){
      break;
    }
    //bucket grow
    if(GetLocalDepthInternal(index)!=GetGlobalDepthInternal()){
      RedistributeBucket(dir_[index]);
    }
    //dir grow,bucket grow in next iteration
    else{
      global_depth_++;
      auto dir_size = dir_.size();
      // dir size * 2
      for(size_t i = 0; i<dir_size; ++i){
        dir_.emplace_back(dir_[i]);
      }
    }

  }
  
}





//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for(auto &[k,v]: list_){
    if(key == k){
      value = v;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  //UNREACHABLE("not implemented");
  
  for (auto it = list_.begin(); it!=list_.end();){
    if(it->first== key){
      list_.erase(it++);
      return true;
    }
    
    ++it;
    
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for(auto &[k,v]: list_){
    if(k == key){
      v = value;
      return true;
    }
  }
  if(IsFull()){
    return false;
  }
  list_.push_back(std::make_pair(key,value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
