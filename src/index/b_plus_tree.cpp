#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
//B+树的结构为：B+树的每个节点都是一个页，页的大小为4KB，每个页的第一个4字节存储父节点的页号，第二个4字节存储下一个兄弟节点的页号，第三个4字节存储节点的大小，第四个4字节存储节点的最大大小，第五个4字节存储节点的最小大小，第六个4字节存储节点的页号，第七个4字节存储节点的类型（叶子节点或者内部节点），第八个4字节存储节点的下一个节点的页号，第九个4字节存储节点的上一个节点的页号，第十个4字节存储节点的父节点的页号，第十一个4字节存储节点的最大大小，第十二个4字节存储节点的最小大小，第十三个4字节存储节点的大小，第十四个4字节存储节点的页号，第十五个4字节存储节点的类型（叶子节点或者内部节点），第十六个4字节存储节点的下一个节点的页号，第十七个4字节存储节点的上一个节点的页号，第十八个4字节存储节点的父节点的页号，第十九个4字节存储节点的最大大小，第二十个4字节存储节点的最小大小，第二十一个4字节存储节点的大小，第二十二个4字节存储节点的页号，第二十三个4字节存储节点的类型（叶子节点或者内部节点），第二十四个4字节存储节点的下一个节点的页号，第二十五个4字节存储节点的上一个节点的页号，第二十六个4字节存储节点的父节点的页号，第二十七个4字
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),//索引ID
      buffer_pool_manager_(buffer_pool_manager),//缓冲池管理器
      processor_(KM),//处理器
      leaf_max_size_(leaf_max_size),//叶子节点的最大大小
      internal_max_size_(internal_max_size) //内部节点的最大大小
      {
  //需要完成的内容：初始化B+树
  Page *root_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);//获取根节点
  if (root_page == nullptr) {//如果根节点为空
    LOG(ERROR) << "Failed to fetch root page";
    return;
  }
  auto index_root_page = reinterpret_cast<IndexRootsPage *>(root_page);//将根节点转换为索引根节点
  index_root_page->GetRootId(index_id_, &root_page_id_);//获取根节点的ID，赋值给根节点ID
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);//解锁根节点
  if(leaf_max_size_ == UNDEFINED_SIZE) {//如果叶子节点的最大大小未定义
    leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId));//叶子节点的最大大小为（页大小-叶子节点头部大小）/（处理器的长度+RowId的大小）
  }
  if(internal_max_size_ == UNDEFINED_SIZE) {//如果内部节点的最大大小未定义
    internal_max_size_ = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(page_id_t));//内部节点的最大大小为（页大小-内部节点头部大小）/（处理器的长度+页ID的大小）
  }

}

void BPlusTree::Destroy(page_id_t current_page_id) {
    if (root_page_id_ == INVALID_PAGE_ID) {
        return;
    }
    if (current_page_id == INVALID_PAGE_ID) {
        current_page_id = root_page_id_;
    }
    Page *page = buffer_pool_manager_->FetchPage(current_page_id);
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (node->IsLeafPage()) {
        buffer_pool_manager_->UnpinPage(current_page_id, true);
        buffer_pool_manager_->DeletePage(current_page_id);
    } else {
        auto *inter = reinterpret_cast<InternalPage *>(node);
        for (int i = 0; i < inter->GetSize(); i++) {
            Destroy(inter->ValueAt(i));
        }
        buffer_pool_manager_->UnpinPage(current_page_id, true);
        buffer_pool_manager_->DeletePage(current_page_id);
    }
    if (current_page_id == root_page_id_) {
        UpdateRootPageId(-1);
    }
}

/*
 * Helper function to decide whether Page_tmp_treenodeent b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;//如果根节点ID为无效ID，则返回true
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key into result
 * This method is used for point query
 * @return : true means key exists
 * key : the key that needs to be searched
 * result : the value associated with input key
 * transaction : the txn that is executing this operation
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  if(IsEmpty()) {//如果B+树为空
    return false;//返回false
  }
  RowId res_tmp;
  auto leaf = reinterpret_cast<LeafPage *>(FindLeafPage(key, INVALID_PAGE_ID, false)->GetData());//查找叶子节点
  bool ret = leaf->Lookup(key, res_tmp,processor_);//使用叶节点的函数找到key并将值存在res_tmp中
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);//解锁叶子节点
  if(ret)result.push_back(res_tmp);
  return ret;//返回查找结果
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if Page_tmp_treenodeent tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 * just use insertleaf to do this
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  if(IsEmpty())
  {StartNewTree(key,value);return 1;}
  // cout << "Not empty" << endl;
  return InsertIntoLeaf(key,value,transaction);
 }
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), 
 * then update b+tree's root page id and insert entry directly into leaf page.
 */
//建立新树
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
    page_id_t newpage_id;
    Page * new_page = buffer_pool_manager_->NewPage(newpage_id);
    if(new_page == nullptr){
      throw("out of memory in StartNewTree");
    }

    //初始化新页
    root_page_id_ = newpage_id;//根节点ID为新页ID
    UpdateRootPageId(1);//更新根节点ID
    auto leaf = reinterpret_cast<LeafPage *>(new_page->GetData());//将新页转换为叶子节点,因为新页是叶子节点
    leaf->Init(newpage_id, INVALID_PAGE_ID, processor_.GetKeySize(),leaf_max_size_);
    leaf->Insert(key, value, processor_);//插入key和value
    buffer_pool_manager_->UnpinPage(newpage_id, true);//解锁新页
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) { 
  //找到叶子节点
  Page *page = FindLeafPage(key);
  auto *leaf = reinterpret_cast<::LeafPage *>(page->GetData());
  //如果key已经存在
  RowId tmp;
  // cout << key << endl;
  // //打印当前的leaf的内容（按照序号打出rowid）
  // for(int i = 0; i < leaf->GetSize(); i++) {
  //    cout << "rowid: " << leaf->ValueAt(i).Get() << endl;
  //    cout << "slotnum " << i << endl;
  // }
  // cout << endl;


  bool ret = leaf->Lookup(key, tmp, processor_);
  if(ret)//如果key已经存在
  {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);//解锁叶子节点
    return false;//返回false
  }
  int size = leaf->Insert(key, value, processor_);//插入key和value
  if(size >= leaf_max_size_){//如果叶子节点的大小大于叶子节点的最大大小
    auto *new_leaf = Split(leaf, transaction);//分裂叶子节点
    InsertIntoParent(leaf, new_leaf->KeyAt(0), new_leaf, transaction);//插入父节点,new_leaf的第一个key
    leaf->SetNextPageId(new_leaf->GetPageId());
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
  return 1;
 }
/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 * node is the input page that needs to be splited
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) { 
  //实现思路：
  //1.首先申请一个新页，如果申请失败则抛出异常
  //2.初始化新页并强制转换为内部页，初始化新页的ID，父节点ID，处理器的大小和内部节点的最大大小
  //3.将一半的key和value移动到新页
  //4.解锁新页，因为node已经被buffer_pool_manager_解锁，所以不需要再解锁
  page_id_t newpage_id;
  Page * new_page = buffer_pool_manager_->NewPage(newpage_id);
  if(new_page == nullptr){
    throw("out of memory in Split");
  }

  auto Internal_page = reinterpret_cast<InternalPage *>(new_page->GetData());
  Internal_page->Init(newpage_id, node->GetParentPageId(), processor_.GetKeySize(), internal_max_size_);//初始化新页
  node->MoveHalfTo(Internal_page, buffer_pool_manager_);//将一半的key和value移动到新页,此时node已经被buffer_pool_manager_解锁，所以不需要再解锁
  buffer_pool_manager_->UnpinPage(newpage_id, true);//解锁新页
  return Internal_page;
  
 }

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  //实现思路与上面的Split(InternalPage *node, Txn *transaction)类似
  page_id_t newpage_id;
  Page * new_page = buffer_pool_manager_->NewPage(newpage_id);
  if(new_page == nullptr){
    throw("out of memory in Split");
  }

  auto leaf = reinterpret_cast<LeafPage *>(new_page->GetData());
  leaf->Init(newpage_id, node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);//初始化新页
  node->MoveHalfTo(leaf);//将一半的key和value移动到新页
  buffer_pool_manager_->UnpinPage(newpage_id, true);//解锁新页
  return leaf;
 }

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  page_id_t parent_id = old_node->GetParentPageId(),old_id = old_node->GetPageId(),new_id = new_node->GetPageId();
  if(parent_id == INVALID_PAGE_ID){//如果父节点ID为无效ID,说明old_node是根节点，需要新建一个根节点
    page_id_t newpage_id;
    Page * new_page = buffer_pool_manager_->NewPage(newpage_id);
    if(new_page == nullptr){
      throw("out of memory in InsertIntoParent");
    }
    auto *Internal_page = reinterpret_cast<InternalPage *>(new_page->GetData());
    Internal_page->Init(newpage_id, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);//初始化新页
    Internal_page->PopulateNewRoot(old_id, key, new_id);//填充新根节点
    root_page_id_ = newpage_id;//根节点ID为新页ID
    UpdateRootPageId(0);
    old_node->SetParentPageId(newpage_id);//设置父节点ID
    new_node->SetParentPageId(newpage_id);//设置父节点ID
    buffer_pool_manager_->UnpinPage(newpage_id, true);//解锁新页
    return;
  }
  Page * parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto * parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int size = parent->InsertNodeAfter(old_id, key, new_id);
  if(size >= internal_max_size_){//如果父节点的大小大于内部节点的最大大小
    auto *new_parent = Split(parent, transaction);//分裂父节点
    InsertIntoParent(parent, new_parent->KeyAt(0), new_parent, transaction);//插入父节点
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(),true);
  return ;
}
 /* REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If Page_tmp_treenodeent tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  //如果B+树为空
  if(IsEmpty())return;
  //找到叶子节点
  Page *page = FindLeafPage(key, INVALID_PAGE_ID, false);
  auto * leaf = reinterpret_cast<LeafPage *>(page->GetData());
  bool del=false;
  int index = leaf->KeyIndex(key, processor_);//找到key的索引
  int size_after_delete = leaf->RemoveAndDeleteRecord(key, processor_);//删除key和value
  if(size_after_delete < leaf->GetMinSize())
  del = CoalesceOrRedistribute<BPlusTree::LeafPage>(leaf, transaction);//判断是否需要合并或者重新分配
  else if(index ==0)//为叶节点的第一个key
  {
    Page *parent_page = buffer_pool_manager_->FetchPage(leaf->GetParentPageId());
    auto parent = reinterpret_cast<InternalPage *>(parent_page);
    page_id_t value = leaf->GetPageId();
    if(parent_page!=nullptr)
    {
      while(parent->IsRootPage()==0 && parent->ValueIndex(value)==0)//找到叶节点的父节点，这是为了
      {
        value = parent->GetPageId();
        parent_page = buffer_pool_manager_->FetchPage(parent->GetParentPageId());
        parent = reinterpret_cast<InternalPage *>(parent_page);
        buffer_pool_manager_->UnpinPage(parent->GetPageId(),false);
      }
      if(parent->ValueIndex(value)!=0 && processor_.CompareKeys(leaf->KeyAt(0), parent->KeyAt(parent->ValueIndex(value))) != 0)
      {
        parent->SetKeyAt(parent->ValueIndex(value), leaf->KeyAt(0));
        buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
      }
    }
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
  if(del)buffer_pool_manager_->DeletePage(page->GetPageId());
  return;
}
/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
//用于检查是否需要合并或者重新分配
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  page_id_t parent_id = node->GetParentPageId();
  if(parent_id == INVALID_PAGE_ID)return AdjustRoot(node);//如果父节点ID为无效ID,说明node是根节点
  //find the parent node
  Page * parent_page = buffer_pool_manager_->FetchPage(parent_id);
  BPlusTree :: InternalPage * parent = reinterpret_cast<BPlusTree :: InternalPage *>(parent_page->GetData());
  page_id_t page_tmp = node ->GetPageId();
  int index = parent->ValueIndex(page_tmp);

  //优先考虑重新分配，从前后兄弟节点中选择一个
  if(index > 0)//可以选择前面一个
  {
    Page * left_page = buffer_pool_manager_->FetchPage(parent->ValueAt(index-1));
    N * left = reinterpret_cast<N *>(left_page->GetData());
    if(left->GetSize() < left->GetMaxSize()){//如果前面一个节点的大小小于前面一个节点的最大大小
      Redistribute(left, node, 1);//重新分配
      auto child = reinterpret_cast<BPlusTreePage *>(node);
      if(child->IsLeafPage() == 0)//如果不是叶子，则需要更新父节点的key
      {
        auto child_internal = reinterpret_cast<InternalPage *>(node);
        page_id_t child_id = child_internal->LeftMostKey(buffer_pool_manager_);
        Page * leaf_page = buffer_pool_manager_->FetchPage(child_id);
        auto leaf = reinterpret_cast<LeafPage *>(leaf_page);
        parent->SetKeyAt(index, leaf->KeyAt(0));
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
      }
      buffer_pool_manager_->UnpinPage(left->GetPageId(),true);//true because we have modified the page
      buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
      return false;
    }
  }
  if(index < parent->GetSize()-1)//可以选择后面一个
  {
    Page * right_page = buffer_pool_manager_->FetchPage(parent->ValueAt(index+1));
    N * right = reinterpret_cast<N *>(right_page->GetData());
    if(right->GetSize() < right->GetMaxSize()){//如果后面一个节点的大小小于后面一个节点的最大大小
      Redistribute(right, node, 0);//重新分配
      auto child = reinterpret_cast<BPlusTreePage *>(node);
      if(child->IsLeafPage() == 0)//如果不是叶子，则需要更新父节点的key
      {
        auto child_internal = reinterpret_cast<InternalPage *>(node);
        page_id_t child_id = child_internal->LeftMostKey(buffer_pool_manager_);
        Page * leaf_page = buffer_pool_manager_->FetchPage(child_id);
        auto leaf = reinterpret_cast<LeafPage *>(leaf_page);
        parent->SetKeyAt(index, leaf->KeyAt(0));
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
      }
      buffer_pool_manager_->UnpinPage(right->GetPageId(),true);//true because we have modified the page
      buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
      return false;
    }
  }

  //如果不能重新分配，则考虑合并
  if(index > 0)//可以选择前面一个
  {
    page_id_t left_id = parent->ValueAt(index-1);
    N * left = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(left_id)->GetData());
    if(left->GetSize() + node->GetSize() <= left->GetMaxSize()){//如果前面一个节点的大小加上当前节点的大小小于前面一个节点的最大大小
      Coalesce(left, node, parent, index-1, transaction);//合并
      buffer_pool_manager_->UnpinPage(left->GetPageId(),true);//true because we have modified the page
      buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
      buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
      return true;
    }
  }
  else{
    Page * right_page = buffer_pool_manager_->FetchPage(parent->ValueAt(index+1));
    N * right = reinterpret_cast<N *>(right_page->GetData());
    if(right->GetSize() + node->GetSize() <= right->GetMaxSize()){//如果后面一个节点的大小加上当前节点的大小小于后面一个节点的最大大小
      Coalesce(right, node, parent, index, transaction);//合并
      buffer_pool_manager_->UnpinPage(right->GetPageId(),true);//true because we have modified the page
      buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
      buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
      return false;
    }
  
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  //进行合并
  node->MoveAllTo(neighbor_node);
  parent -> Remove(index);
  if (parent->GetSize() < parent->GetMinSize())return CoalesceOrRedistribute<BPlusTree::InternalPage>(parent, transaction);
  
  return false;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  //进行合并
  node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
  parent -> Remove(index);
  if (parent->GetSize() < parent->GetMinSize())//如果父节点的大小小于父节点的最小大小return CoalesceOrRedistribute<BPlusTree::LeafPage>(parent, transaction);
  
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  page_id_t parent_id = node->GetParentPageId();
  auto * parent = reinterpret_cast<BPlusTree::InternalPage *>(buffer_pool_manager_->FetchPage(parent_id)->GetData());

  if(!index)
  {
    neighbor_node->MoveFirstToEndOf(node);
    int node_index = parent->ValueIndex(neighbor_node->GetPageId());//因为nei的第一个key被移动到node的最后，所以需要更新父节点的key
    parent->SetKeyAt(node_index, neighbor_node->KeyAt(0));
  }
  else {
    neighbor_node->MoveLastToFrontOf(node);
    int node_index = parent->ValueIndex(node->GetPageId());//因为nei的最后一个key被移动到node的最前，所以需要更新父节点的key
    parent->SetKeyAt(node_index, node->KeyAt(0));}
  buffer_pool_manager_->UnpinPage(parent_id, true);

}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  page_id_t parent_id = node->GetParentPageId();
  Page * parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto * parent = reinterpret_cast<BPlusTree::InternalPage *>(parent_page->GetData());

  if(!index)
  {
    neighbor_node->MoveFirstToEndOf(node, parent->KeyAt(index), buffer_pool_manager_);
    int node_index = parent->ValueIndex(neighbor_node->GetPageId());//因为nei的第一个key被移动到node的最后，所以需要更新父节点的key
    parent->SetKeyAt(node_index, neighbor_node->KeyAt(0));
  }
  else {
    neighbor_node->MoveLastToFrontOf(node, parent->KeyAt(index), buffer_pool_manager_);
    int node_index = parent->ValueIndex(node->GetPageId());//因为nei的最后一个key被移动到node的最前，所以需要更新父节点的key
    parent->SetKeyAt(node_index, node->KeyAt(0));}
  buffer_pool_manager_->UnpinPage(parent_id, true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method in case that root page is
 * empty,which means root page should be deleted.
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree，这种情况下，root page的大小为0，root page没有孩子，因此应该删除root page，返回true
 * 这两个case的区别在于root page是否有孩子，即root page是否是叶子节点
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
    int size = old_root_node->GetSize();
    if(size > 1)return false;
    //如果root page是叶子节点
    if(old_root_node->IsLeafPage()) {
      if(size == 0) {
        root_page_id_ = INVALID_PAGE_ID;
        UpdateRootPageId(0);//更新根节点ID为无效ID
        return true;
      }
      return false;
    }
    //如果root page是内部节点，需要找到root page的孩子
  auto root_page = reinterpret_cast<BPlusTree::InternalPage *>(old_root_node);
  root_page_id_ = root_page->RemoveAndReturnOnlyChild();//删除root page的孩子，并返回孩子的ID，将这个孩子设置为新的root page
  Page * new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto new_root = reinterpret_cast<BPlusTree::InternalPage *>(new_root_page->GetData());
  new_root->SetParentPageId(INVALID_PAGE_ID);//设置新根节点的父节点ID为无效ID
  UpdateRootPageId(0);//更新根节点ID
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  Page * left_most_page = FindLeafPage(nullptr, INVALID_PAGE_ID, true);
  page_id_t page_id = left_most_page->GetPageId();
  if(left_most_page == nullptr)return IndexIterator();
  buffer_pool_manager_->UnpinPage(page_id, false);
  return IndexIterator(page_id, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
   Page * left_most_page = FindLeafPage(key, INVALID_PAGE_ID, false);
  page_id_t page_id = left_most_page->GetPageId();
  if(left_most_page == nullptr)return IndexIterator();
  auto * leaf = reinterpret_cast<BPlusTree::LeafPage *>(left_most_page);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return IndexIterator(page_id, buffer_pool_manager_,leaf->KeyIndex(key, processor_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator();//返回一个空的迭代器,表示结束
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if (IsEmpty()) return nullptr;

  Page *Page_tmp = buffer_pool_manager_->FetchPage(root_page_id_);
  page_id_t Page_id_tmp = root_page_id_;//当前页的ID设置为根节点的ID，这是为了从根节点开始查找
  auto *Page_tmp_treenode = reinterpret_cast<BPlusTreePage *>(Page_tmp->GetData());

  while (Page_tmp_treenode->IsLeafPage()==0) {
    buffer_pool_manager_->UnpinPage(Page_id_tmp, false);  // 每找一层关闭上一层的内节点page
    auto *internalPage = reinterpret_cast<BPlusTree::InternalPage *>(Page_tmp);  // 打开上一层内节点page
    if (leftMost)Page_id_tmp = internalPage->ValueAt(0);//如果leftMost为true,则找到最左边的叶子节点
    else Page_id_tmp = internalPage->Lookup(key, processor_);//否则根据key找到对应的孩子

    Page_tmp = buffer_pool_manager_->FetchPage(Page_id_tmp);  // 改变当前页的指针
    Page_tmp_treenode = reinterpret_cast<BPlusTreePage *>(Page_tmp->GetData());
  }
  
  return Page_tmp;
}

/*
 * Update/Insert root page id in IndexRootsPage(where page_id = 0, index_roots__page is
 * defined under include/page/index_roots__page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, Page_tmp_treenodeent_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  //找到root page
  auto root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  if(insert_record)root_page->Insert(index_id_, root_page_id_);//插入root page的ID
  else root_page->Update(index_id_, root_page_id_);//更新root page的ID
  buffer_pool_manager_->UnpinPage(1, true);//解锁root page
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}
