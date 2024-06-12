#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) : cache(num_pages, lru_list.end()) {
  num_page = num_pages;
}

LRUReplacer::~LRUReplacer() = default;
/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (lru_list.empty()!=0)//如果为空则返回false
   {
    return false;
  }
  else {
    *frame_id = lru_list.back();//返回最近最少被访问的页
    cache[*frame_id] = lru_list.end();//将其从lru_list_中移除
    lru_list.pop_back();//删除
    return true;
  }
}
/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  auto temp = cache[frame_id];
  if (temp != lru_list.end()) {
    // 存在对应元素
    lru_list.erase(temp);
    cache[frame_id] = lru_list.end();
  }
}
/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (lru_list.size() >= num_page ) {
    return;
  }
  else if (cache[frame_id] != lru_list.end()){
    return ;
  }
  else{
    lru_list.push_front(frame_id);
    cache[frame_id] = lru_list.begin();
  }

}
/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() { return lru_list.size(); }