//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::GetAvailableFrame(frame_id_t *out_frame_id) -> bool {
  frame_id_t fid;
  // first ask free list for free frame:
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
    *out_frame_id = fid;
    return true;
  }
  // if no free frame,just use lru-k replacer to evict one frame
  if (replacer_->Evict(&fid)) {
    // need check isdirty flag
    if (pages_[fid].is_dirty_) {
      disk_manager_->WritePage(pages_[fid].page_id_, pages_[fid].GetData());
      pages_[fid].is_dirty_ = false;
    }
    page_table_->Remove(pages_[fid].GetPageId());
    *out_frame_id = fid;
    return true;
  }
  return false;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  if (!GetAvailableFrame(&frame_id)) {
    return nullptr;
  }
  // you can get the frame_id
  page_id_t new_page_id;
  new_page_id = AllocatePage();
  pages_[frame_id].page_id_ = new_page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].ResetMemory();
  page_table_->Insert(new_page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  //"Pin" the frame by calling replacer.SetEvictable(frame_id, false)
  replacer_->SetEvictable(frame_id, false);
  Page *new_page = &pages_[frame_id];
  *page_id = new_page->GetPageId();  // return the page_id pointer
  return new_page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    // if in buffer pool: pin_count++
    ++pages_[frame_id].pin_count_;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  // need search from disk and update all
  if (!GetAvailableFrame(&frame_id)) {
    return nullptr;
  }
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].ResetMemory();
  page_table_->Insert(page_id, frame_id);
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].pin_count_ <= 0) {
    // If page_id is not in the buffer pool or its pin count is already 0, return false.
    return false;
  }
  --pages_[frame_id].pin_count_;  // Decrement the pin count of a page.
  // If the pin count reaches 0, the frame should be evictable by the replacer
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  if (!pages_[frame_id].is_dirty_) {
    // modify the status only when it is clean otherwise it can't be set
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> guard(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  if (pages_[frame_id].GetPinCount() > 0) {
    // page is pinned and cannot be deleted
    return false;
  }
  if (pages_->is_dirty_) {
    disk_manager_->WritePage(page_id, pages_[frame_id].data_);
    pages_[frame_id].is_dirty_ = false;
  }
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;

  page_table_->Remove(page_id);
  pages_[frame_id].ResetMemory();     // reset the page's memory and metadata.
  replacer_->Remove(frame_id);        // stop tracking the frame in the replacer
  free_list_.emplace_back(frame_id);  // add the frame back to the free list
  DeallocatePage(page_id);            // freeing the page on the disk
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
