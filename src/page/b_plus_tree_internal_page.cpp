#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 * The structure of an internal page is as follows:
 * --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 * --------------------------------------------------------------------------
 * The first key is always invalid, so any search/lookup should ignore the first key.
 * The middle key is stored in the parent page, which is the first key in this page.
 * Header includes page type, size, page id, parent id, max size, key size and LSN.
 * The key and page_id pairs are store
 * d in increasing order.
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetKeySize(key_size);
  SetLSN(INVALID_LSN);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}
//用于设置key
void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) //用于设置value
{
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
    if(GetSize() == 1) return ValueAt(0);//如果只有一个key，直接返回第一个value
    if(GetSize( )==0)return INVALID_PAGE_ID;//如果没有key，返回INVALID_PAGE_ID
    if (KM.CompareKeys(key, KeyAt(1)) < 0) return ValueAt(0);
    //find the first key that is greater than the input key
    int left = 1, right = GetSize() ;
    while(left < right-1) {
        int mid = (left + right) / 2;
        if(KM.CompareKeys(KeyAt(mid), key) < 0) left = mid ;
        else if(KM.CompareKeys(KeyAt(mid), key) > 0) right = mid ;
        else return ValueAt(mid);
    }
    return ValueAt(left); 
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  //当一个节点分裂时，需要创建一个新的根节点，将原来的根节点的第一个key和新的key作为新的根节点的key
  SetSize(2);
  SetValueAt(0, old_value);//将原来的根节点的pageId放在第一个位置
  SetKeyAt(1, new_key);SetValueAt(1, new_value);//将新的key和value放在第二个位置
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int tmp_size = GetSize();
  SetSize(tmp_size+1);
  int old_Pos = ValueIndex(old_value);
  PairCopy(PairPtrAt(old_Pos+2), PairPtrAt(old_Pos+1), tmp_size-old_Pos-1);//将old_Pos+1之后的key-value对往后移动一位
  SetValueAt(old_Pos+1,new_value);
  SetKeyAt(old_Pos+1,new_key);
  return GetSize();
}

void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int half = GetSize() / 2;//将当前节点的key-value对分成两半
  recipient->CopyNFrom(PairPtrAt(half), GetSize() - half, buffer_pool_manager);//将当前节点的后半部分的key-value对复制到recipient中
  SetSize(half);//将当前节点的size设置为half
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
//Prob
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) //将src中的size个key-value对复制到当前节点中
{
  char *source = reinterpret_cast<char *>(src);

  for (int i = 0; i < size; i++) {
    GenericKey *key = reinterpret_cast<GenericKey *>(source);//将src中的key复制到key中
    source += GetKeySize();
    page_id_t *pageId = reinterpret_cast<page_id_t *>(source);//将src中的pageId复制到pageId中
    source += sizeof(page_id_t);
    CopyLastFrom(key, *pageId, buffer_pool_manager);//将key和pageId复制到当前节点中
  }

}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  int tmp_size = GetSize();
  for ( int i=index; i<tmp_size-1; i++ ) {
    SetValueAt(i, ValueAt(i+1));
    SetKeyAt(i, KeyAt(i+1));
  }
  SetSize(tmp_size-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  if(GetSize()!=1)return INVALID_PAGE_ID;
  SetKeyAt(0, nullptr);
  page_id_t value = ValueAt(0);
  SetValueAt(0, INVALID_PAGE_ID);
  SetSize(0);
  return value;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */

//middle_key是父节点中的key
//将当前节点中的key-value对全部复制到recipient中
//将middle_key和ValueAt(0)复制到recipient的最后，因为ValueAt(0)是左子树的pageId
//middle_key存储在父节点中，是当前节点的第一个key
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  int tmp_size = GetSize();
  recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);//将middle_key和ValueAt(0)复制到recipient的最后，因为ValueAt(0)是左子树的pageId
  recipient->CopyNFrom(PairPtrAt(1), GetSize() - 1, buffer_pool_manager);//将当前节点中的key-value对复制到recipient中
  buffer_pool_manager->DeletePage(GetPageId());//删除当前节点
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,BufferPoolManager *buffer_pool_manager) {
  page_id_t middle_key_value = ValueAt(0);
  recipient -> CopyLastFrom(middle_key, middle_key_value, buffer_pool_manager);
  Remove(0);//删除当前节点中的第一个key-value对
}
//--------------------------------------------------------------------------------------------------------------------------------
/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(GetSize(),key);
  SetValueAt(GetSize(),value);
  SetSize(GetSize()+1);
  Page *page = buffer_pool_manager->FetchPage(value);//在缓冲池中找到对应的page
  auto *inPage = reinterpret_cast<InternalPage *>(page->GetData());//转换成InternalPage
  inPage->SetParentPageId(GetPageId());//修改parentId为当前页的pageId
  buffer_pool_manager->UnpinPage(value, true);//将修改后的page放回缓冲池
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  recipient -> CopyFirstFrom(ValueAt(GetSize()-1), buffer_pool_manager);
  recipient -> SetKeyAt(1, middle_key);//将middle_key放在第一个位置
  Remove(GetSize()-1);
} 

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  InsertNodeAfter(INVALID_PAGE_ID, KeyAt(0),value);//将当前节点的第一个key-value对复制到当前节点的第二个位置
  auto *inPage = reinterpret_cast<InternalPage *>(buffer_pool_manager->FetchPage(value)->GetData());//转换成InternalPage
  inPage->SetParentPageId(GetPageId());//修改parentId为当前页的pageId
  buffer_pool_manager->UnpinPage(value, true);//将修改后的page放回缓冲池
}

/*
 * Return the left most key of the page.
 */
//added function to get the left most key
page_id_t InternalPage::LeftMostKey(BufferPoolManager *buffer_pool_manager){
  //获取当前的pageId
  Page *page = buffer_pool_manager->FetchPage(this->GetPageId());
  auto *inPage = reinterpret_cast<InternalPage *>(page->GetData());
  InternalPage * intPage;
  while(inPage->IsLeafPage()!=true){
    buffer_pool_manager->UnpinPage(page->GetPageId(), false); //unpin page
    intPage = reinterpret_cast<InternalPage *>(page);
    page = buffer_pool_manager->FetchPage(intPage->ValueAt(0));
    inPage = reinterpret_cast<InternalPage *>(page->GetData());//转换成InternalPage
  }
  buffer_pool_manager->UnpinPage(page->GetPinCount(),false);
  return intPage->ValueAt(0);
}