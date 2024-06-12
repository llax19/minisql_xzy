#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;//默认构造函数

//The iterator is initialized to the first key/value pair in the given page.
//It is used to iterate over the key/value pairs in the page.

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}
// Return the key/value pair this iterator is currently pointing at.
std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  return page->GetItem(item_index);
}
// Move to the next key/value pair and return the iterator 
IndexIterator &IndexIterator::operator++() {

  if(item_index != page->GetSize() - 1) item_index++;
  else{
    int next_id = page->GetNextPageId();
    //unpin上一个page
    buffer_pool_manager->UnpinPage(page->GetPageId(), false);
    page = reinterpret_cast<IndexIterator::LeafPage *>(buffer_pool_manager->FetchPage(next_id));
//    if(page== nullptr){
//      auto end = new IndexIterator;
//      return *end;
//    }
    item_index = 0;
    current_page_id = next_id;
  }
  return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}