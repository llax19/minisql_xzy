#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Insert the tuple into the page.
  // Step3: If the tuple is too large, return false.
  // Step4: If the tuple is successfully inserted, return true.
  uint32_t size = row.GetSerializedSize(schema_);
  if (size >= PAGE_SIZE) return false;

  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));//找到第一个page
  if(page == nullptr) return false;//如果page为空，返回false
  page->WLatch();//获取写锁
  int result = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);//插入tuple
  page->WUnlatch();//释放写锁
  buffer_pool_manager_->UnpinPage(first_page_id_, true);//解锁
  while(!result){//如果插入失败
    page_id_t next_id = page->GetNextPageId();//获取下一个page的id
    if(next_id != INVALID_PAGE_ID){//如果有下一个page
      page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_id));//获取下一个page
      if(page == nullptr) return false;//如果page为空，返回false
      page->WLatch();//获取写锁
      result = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);//插入tuple
      page->WUnlatch();//释放写锁
      buffer_pool_manager_->UnpinPage(next_id, true);//解锁
    }else{//如果没有下一个page
    //创建新的page
      page_id_t new_pid;
      buffer_pool_manager_->NewPage(new_pid);//在buffer_pool_manager中创建新的page
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(new_pid));//获取新的page
      new_page->WLatch();//获取写锁
      new_page->Init(new_pid, INVALID_PAGE_ID, log_manager_, txn);//初始化新的page
      new_page->SetNextPageId(INVALID_PAGE_ID);//设置下一个page的id
      page->WLatch();//获取写锁
      page->SetNextPageId(new_pid);//设置下一个page的id
      page->WUnlatch();//释放写锁
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);//解锁
      result = new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);//插入tuple
      new_page->WUnlatch();//释放写锁
      buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);//解锁
      break;
    }
  }
  if(result) return true;//如果插入成功，返回true
  return false;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) { 
  auto page_old = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));//获取page
  page_old->WLatch();//获取写锁
  Row old_row = Row(rid);//创建一个空的row
//  page_old->GetTuple(&old_row,schema_,txn,lock_manager_);只要有rid就行
  bool update_result = page_old->UpdateTuple(row,&old_row,schema_,txn,lock_manager_,log_manager_);
  //要求old_row的field是空的
  page_old->WUnlatch();
  buffer_pool_manager_->UnpinPage(page_old->GetPageId(), true);//在buffer_pool_manager中解锁
  return update_result;
 }

/**
 * TODO: Student Implement
 */
//在物理上删除tuple
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  // Step3: If the tuple is successfully deleted, return true.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));//获取page
  page->WLatch();//获取写锁
  page->ApplyDelete(rid, txn, log_manager_);//调用page的ApplyDelete删除tuple
  page->WUnlatch();//释放写锁
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);//在buffer_pool_manager中解锁
  
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  //获取RowId为row->rid_的记录；
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));//获取page
  page->RLatch();//获取读锁
  bool get_result = page->GetTuple(row,schema_,txn,lock_manager_);//调用page的GetTuple获取tuple 
  page->RUnlatch();//释放读锁
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);//在buffer_pool_manager中解锁
  return get_result;
}


void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) { 
  //获取堆表的首迭代器；
  auto page_tmp = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));//获取page
  page_tmp->RLatch();//获取读锁
  RowId rid;
  page_tmp->GetFirstTupleRid(&rid);//获取第一个tuple的RowId
  page_tmp->RUnlatch();//释放读锁
  return TableIterator(this, rid, txn);
 }

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { 
  //获取堆表的尾迭代器；
  return TableIterator(this, RowId(), nullptr);
 }
