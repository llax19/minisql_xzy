#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};
//这一部分的代码是实现BufferPoolManager的功能
//BufferPoolManager的功能是管理buffer pool，即缓冲池
//缓冲池是内存中的一块区域，用于存放磁盘中的数据
//缓冲池的大小是固定的，由pool_size_指定
//缓冲池中的每一页都是Page类的实例
//缓冲池中的每一页都有一个唯一的page_id
//缓冲池中的每一页都有一个pin_count，表示有多少个指针指向这一页
//缓冲池中的每一页都有一个is_dirty，表示这一页是否被修改过
//缓冲池中的每一页都有一个data_，表示这一页的数据
//缓冲池中的每一页都有一个page_table_，表示这一页在缓冲池中的位置
BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

// 1.     Search the page table for the requested page (P).
// 1.1    If P exists, pin it and return it immediately.
// 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
//        Note that pages are always found from the free list first.
// 2.     If R is dirty, write it back to the disk.
// 3.     Delete R from the page table and insert P.
// 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  //page_id判断
  // return nullptr;
  if(page_id > MAX_VALID_PAGE_ID || page_id <= INVALID_PAGE_ID) return nullptr;
  //latch_.lock();
  //若在page_table中找到了page_id
  if(page_table_.count(page_id)!=0){
    frame_id_t tmp = page_table_[page_id];//找到了page_id
    pages_[tmp].pin_count_++;//pin++
    replacer_->Pin(tmp);//
    //latch_.unlock();
    return &pages_[tmp];
  }
  //若在page_table中没有找到page_id
  frame_id_t tmp;
  if(free_list_.size()<=0)//free_list为空
  {
    if(replacer_->Victim(&tmp)==false) return nullptr;//replacer也没有
    if(pages_[tmp].IsDirty())//dirty
    {
      disk_manager_->WritePage(pages_[tmp].GetPageId(),pages_[tmp].GetData());
    }
    //page_table_.erase(pages_[tmp].page_id_);//删除page_id
  }
  else//free_list不为空
  {
    tmp = free_list_.front();//取出free_list的第一个
    free_list_.pop_front();//删除第一个
  }
  page_table_[page_id] = tmp;//插入page_table
  //Update P's metadata, read in the page content from disk, and then return a pointer to P.
  pages_[tmp].pin_count_ = 1;
  pages_[tmp].page_id_ = page_id;
  
  disk_manager_->ReadPage(page_id, pages_[tmp].data_);
  //latch_.unlock();
  // return nullptr;
  return &pages_[tmp];
}

// 0.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
// 1.   If all the pages in the buffer pool are pinned, return nullptr.
// 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
// 3.   Update P's metadata, zero out memory and add P to the page table.
// 4.   Set the page ID output parameter. Return a pointer to P.
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  if(free_list_.size()>0)//在free_list中存在
  {
    frame_id_t tmp = free_list_.front();
    free_list_.pop_front();
    page_id = AllocatePage();
    page_table_[page_id] = tmp;
    pages_[tmp].page_id_ = page_id;
    pages_[tmp].pin_count_ = 1;
    pages_[tmp].is_dirty_ = false;
    memset(pages_[tmp].data_,0,PAGE_SIZE);
    return &pages_[tmp];
  }
  //从replacer中找
  frame_id_t tmp ;//找到的frame_id
  if(replacer_->Victim(&tmp)==false) return nullptr;//replacer也没有
  if(pages_[tmp].IsDirty())//dirty
  {
    disk_manager_->WritePage(pages_[tmp].GetPageId(),pages_[tmp].GetData());//写回磁盘
  }
  page_id = AllocatePage();//分配新页面
  page_table_[page_id] = tmp;//插入page_table
  page_table_.erase(pages_[tmp].page_id_);//删除page_id,因为已经分配了新的page_id
  //Update P's metadata, read in the page content from disk, and then return a pointer to P.
  pages_[tmp].ResetMemory();
  pages_[tmp].page_id_ = page_id;
  pages_[tmp].pin_count_ = 1;
  pages_[tmp].is_dirty_ = false;
  return &pages_[tmp];
}
// 0.   Make sure you call DeallocatePage!
// 1.   Search the page table for the requested page (P).
// 1.   If P does not exist, return true.
// 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
// 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  if(page_table_.count(page_id)==0) return true;//不存在
  frame_id_t tmp = page_table_[page_id];//找到了page_id

  if(pages_[tmp].pin_count_>0) return false;//pin_count>0
  page_table_.erase(page_id);//从page_table中删除，因为page_table用于跟踪页面的元数据
  pages_[tmp].page_id_=INVALID_PAGE_ID;
  pages_[tmp].ResetMemory();//重置metadata
  free_list_.push_back(tmp);//把tmp放回free_list，因为删除了，已经是空闲的了
  DeallocatePage(page_id);//释放page_id
  return true;
 
}
//实现思路：
//1.首先判断page_id是否在page_table中，如果不在则返回false
//2.如果在page_table中，则找到对应的frame_id
//3.如果找到了frame_id，则将对应的pin_count减1
//4.如果pin_count减1后为0，则将该frame_id插入到replacer_中
//5.返回true
//replacer，free_list，page_table都是用于管理缓冲池的数据结构，
//区别在于replacer是用于管理缓冲池中的页面替换策略，free_list是用于管理缓冲池中的空闲页面，page_table是用于管理缓冲池中的页面元数据
//空闲的页面可以被替换，而被替换的页面可以被删除
//frame和page的区别在于frame是缓冲池中的一个页面，page是磁盘中的一个页面
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) //取消页面的引用，若is_dirty为true，则表示页面被修改过，需要写回磁盘
{
  if(page_table_.count(page_id)==0) return false;//不在page_table中,无法unpin
  frame_id_t tmp = page_table_[page_id];//找到了page_id
  pages_[tmp].pin_count_--;//pin_count--
  if(pages_[tmp].pin_count_==0) replacer_->Unpin(tmp);//pin_count为0，插入replacer_
  if(is_dirty) pages_[tmp].is_dirty_ = true;//dirty
  return true;
}
//实现思路：
//1.首先判断page_id是否在page_table中，如果不在则返回false
//2.如果在page_table中，则找到对应的frame_id
//3.如果找到了frame_id，则将对应的数据写入磁盘
//4.返回true
//flush_page的作用是将缓冲池中的页面刷新到磁盘上
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  latch_.lock();//加锁,因为要修改page_table_，如果没有加锁，可能会出现多线程访问的问题
  frame_id_t tmp = page_table_[page_id];
  if(page_table_.count(page_id)>0){
    disk_manager_->WritePage(page_id, pages_[tmp].data_);
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}