#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {

  if (page_allocated_ == MAX_CHARS*8)
    {
        return false;
    } else {
        // 分配下一个空闲的数据页
        page_offset = next_free_page_;
        // 已经分配的数据页数目加 1
        page_allocated_++;

        /*
         * 更改位图中的信息，将记录中对应的 位 置为 1
         */

        // 获取位关于对应字节的偏移量
        int offset = next_free_page_ % 8;

        // 将 temp 中对应的位置为 1
        unsigned char temp = 1 << (7 - offset);

        // 将位所在的字节与 temp 进行或运算，从而实现将对应的位置为 1
        bytes[next_free_page_ / 8] |= temp;

        /*
         * 接下来寻找下一个未分配的页
         */

        if (page_allocated_ == MAX_CHARS*8)
        {
            // 表明当前已经不存在未分配的页

        } else {
            for (unsigned int i = 0; i < MAX_CHARS; i++)
            {
                bool flag = false;  // 记录在当前字节是否存在未分配的页
                temp = 0b10000000;
                for (int j = 0; j < 8; j++)
                {
                    if ((bytes[i] & temp) == 0)
                    {
                        // 表明当前位的值为 0，即存在一个未分配的页
                        flag = true;
                        next_free_page_ = i * 8 + j;
                        break;
                    } else {
                        temp >>= 1;
                    }
                }

                if (flag)
                {
                    // 在当前字节存在未分配的页，循环结束
                    break;
                }
            }
        }

        // 返回申请空间成功的信息
        return true;
    }
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  // 1. Check if the page is already free
  if (IsPageFree(page_offset)) {
    return false;
  }
  if (page_offset >= MAX_CHARS*8)
    {
        return false;
    }

    // 2. Deallocate the page
    // 获取位关于对应字节的偏移量
    unsigned int offset = page_offset % 8;

    // 将 temp 中对应的位置为 1
    unsigned char temp = 1 << (7 - offset);

    // 按位取反
    temp = ~temp;

    // 将对应的位置为 0
    bytes[page_offset / 8] &= temp;

    if (page_allocated_ == 8 * MAX_CHARS)
    {
        // 表明删除之前数据页已经全部被分配，不存在空页
        // 删除之后要更新空页的位置
        next_free_page_ = page_offset;
    }

    // 数据页分配数量减 1
    page_allocated_ --;
    // 返回删除成功的信息
    return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if(page_offset >= MAX_CHARS*8)
    {
        return false;
    } else {
        // 获取位关于对应字节的偏移量
        int offset = page_offset % 8;

        // 将 temp 中对应的位置为 1
        unsigned char temp = 1 << (7 - offset);

        // 将位所在的字节与 temp 进行与运算，从而实现将对应的位置为 1
        return (bytes[page_offset / 8] & temp) == 0;
    }
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;