//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <exception>
#include "common/config.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}



auto LRUKReplacer::CompareFrame(frame_id_t f1, frame_id_t f2) -> bool{
    //return true if evict f1
    if((hash_[f1].time_.size() < k_) && (hash_[f2].time_.size() == k_)){
        return true;
    }
    if ((hash_[f1].time_.size() == k_) && (hash_[f2].time_.size()< k_)){
        return false;
    }
    //remove earlier timestamp
    return hash_[f1].time_.front() < hash_[f2].time_.front();

}


auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
    std::scoped_lock<std::mutex> lock(latch_);
    *frame_id = -1;
    for(auto &[k,v] : hash_){
        if(v.evictable_){
            if(*frame_id == -1 || CompareFrame(k, *frame_id)){
                *frame_id = k;
            }
        }   
    }
    if(*frame_id != -1){
        Remove(*frame_id);
        // hash_.erase(*frame_id);
        // curr_size_--;
        return true;
    }

    return false;
   

    
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    //frame invalid
    if(frame_id > static_cast<int>(replacer_size_)){
        throw "frame invalid";
    }
    if(hash_[frame_id].time_.size() == k_){
        hash_[frame_id].time_.pop();
    }
    hash_[frame_id].time_.push(current_timestamp_++);


}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::scoped_lock<std::mutex> lock(latch_);
    //not found
    if(hash_.count(frame_id) == 0){
        return;
    }
    bool pre = hash_[frame_id].evictable_;
    hash_[frame_id].evictable_ = set_evictable;
    if(pre && !set_evictable){
        curr_size_--;
    }
    else if(!pre && set_evictable){
        curr_size_++;
    }

}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    //std::scoped_lock<std::mutex> lock(latch_);
    //no need to add lock here, evict call this function
    //frame not found
    if(hash_.count(frame_id) == 0){
        return;
    }
    if(!hash_[frame_id].evictable_){
        throw "remove a non-evivtable frame";
    }
    hash_.erase(frame_id);
    curr_size_--;

}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
