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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {

}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
     std::lock_guard<std::mutex> guard(latch_);
     bool flag= false;
     if(!history_list_.empty()){
        for (auto  it = history_list_.rbegin(); it != history_list_.rend(); it++)
        {
            if(entrys_[*it].evictable){
                *frame_id=*it;
                history_list_.erase(std::next(it).base());
                flag=true;
                break;
            }
        }
     }
    if(!flag&&!cache_list_.empty()){
        for (auto  it = cache_list_.rbegin(); it != cache_list_.rend(); it++)
        {
            if(entrys_[*it].evictable){
                *frame_id=*it;
                cache_list_.erase(std::next(it).base());
                flag=true;
                break;
            }
        }
    }
    if(flag){
        entrys_.erase(*frame_id);
        curr_size_--;
        return flag;
    }
    return false; 
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(latch_);
    if(frame_id>static_cast<int> (replacer_size_)){
        throw std::invalid_argument(std::string("Invalid frame_id")+std::to_string(frame_id));
    }
    size_t new_count =++entrys_[frame_id].hit_count_;
    if(new_count==1){
        //new need add into history list
        curr_size_++;
        history_list_.emplace_front(frame_id);
        entrys_[frame_id].pos_=history_list_.begin();
    }
    else{
        if(new_count==k_){
            //need erase from hisory and add into cache
            history_list_.erase(entrys_[frame_id].pos_);
            cache_list_.emplace_front(frame_id);
            entrys_[frame_id].pos_=cache_list_.begin();
        }
        else if(new_count>k_){
            cache_list_.erase(entrys_[frame_id].pos_);
            cache_list_.emplace_front(frame_id);
            entrys_[frame_id].pos_=cache_list_.begin();
        }
        //if new_count<k_,just remain 
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
     std::lock_guard<std::mutex> guard(latch_);                   
     if(entrys_[frame_id].evictable&&!set_evictable){
        curr_size_--;
     }
     else if(!entrys_[frame_id].evictable&&set_evictable){
        curr_size_++;
     }
     entrys_[frame_id].evictable=set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(latch_);
    if(entrys_.find(frame_id)==entrys_.end()) return;
    if(!entrys_[frame_id].evictable){
        throw std::logic_error(std::string("Can't remove an inevictable frame")+std::to_string(frame_id));
    }
    if(entrys_[frame_id].hit_count_<k_) history_list_.erase(entrys_[frame_id].pos_);
    else cache_list_.erase(entrys_[frame_id].pos_);
    curr_size_--;
    entrys_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { 
    return curr_size_; }
}  // namespace bustub
