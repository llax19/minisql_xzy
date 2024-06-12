#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
  tableHeap_ = table_heap;
  currentRowID_ = rid;
}

TableIterator::TableIterator(const TableIterator &other) {
  tableHeap_ = other.tableHeap_;
  currentRowID_ = other.currentRowID_;
}

TableIterator::~TableIterator() {

}

bool TableIterator::operator==(const TableIterator &itr) const {
  return currentRowID_ == itr.currentRowID_ && itr.tableHeap_ == tableHeap_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(currentRowID_ == itr.currentRowID_ && itr.tableHeap_ == tableHeap_);
}

const Row &TableIterator::operator*() {
  ASSERT(tableHeap_, "TableHeap is nullptr.");
  Row* row = new Row(currentRowID_);
  tableHeap_->GetTuple(row, nullptr);
  ASSERT(row, "Invalid row.");
  return *row;
}

Row *TableIterator::operator->() {
  ASSERT(tableHeap_, "TableHeap is nullptr.");
  Row* row = new Row(currentRowID_);
  tableHeap_->GetTuple(row, nullptr);
  ASSERT(row, "Invalid row.");
  return row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  tableHeap_ = itr.tableHeap_;
  currentRowID_ = itr.currentRowID_;
  return *this;
}

TableIterator &TableIterator::operator++() {  // 前置++
  BufferPoolManager *buffer_pool_manager_ = tableHeap_->buffer_pool_manager_;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(currentRowID_.GetPageId()));
  page->RLatch();
  ASSERT(page != nullptr, "page is null");
  RowId new_rid;
  if(!page->GetNextTupleRid(currentRowID_,&new_rid))
  {
    while(page->GetNextPageId()!=INVALID_PAGE_ID)
    {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
      page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
      page->RLatch();
      if(page->GetFirstTupleRid(&new_rid))
        break;
    }
  }
  currentRowID_ = new_rid;//
  if(tableHeap_->End()!=*this)
    tableHeap_->End().currentRowID_=new_rid;
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(currentRowID_.GetPageId(),false);
  return *this;
}
TableIterator TableIterator::operator++(int) {
  TableHeap* tableHeap = tableHeap_;
  RowId currentRowID = currentRowID_;
  ++(*this);
  return TableIterator(tableHeap, currentRowID, nullptr);
}
