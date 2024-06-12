#include "record/schema.h"

/**
用于表示一个数据表或是一个索引的结构。一个Schema由一个或多个的Column构成；
 */
uint32_t Schema::SerializeTo(char *buf) const {
  //本函数用于实现将Schema对象序列化到buf中
  uint32_t offset = 0;
  //写入magic number  
  memcpy(buf + offset, &SCHEMA_MAGIC_NUM, sizeof(SCHEMA_MAGIC_NUM));
  offset += sizeof(SCHEMA_MAGIC_NUM);
  //写入列数
  uint32_t column_count = columns_.size();
  memcpy(buf + offset, &column_count, sizeof(column_count));
  offset += sizeof(column_count);
  //写入每一列
  for (const auto &column : columns_) {
    offset += column->SerializeTo(buf + offset);
  }
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  //实现获取Schema对象序列化后的大小
  uint32_t size = sizeof(SCHEMA_MAGIC_NUM) + sizeof(uint32_t);
  for (const auto &column : columns_) {
    size += column->GetSerializedSize();
  }
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  //实现从buf中反序列化出Schema对象
  uint32_t offset = 0;
  uint32_t magic_num;
  memcpy(&magic_num, buf + offset, sizeof(magic_num));
  offset += sizeof(magic_num);
  ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Magic number does not match.");

  uint32_t column_count;
  memcpy(&column_count, buf + offset, sizeof(column_count));
  offset += sizeof(column_count);

  std::vector<Column *> columns;
  columns.reserve(column_count);
  for (uint32_t i = 0; i < column_count; i++) {
    Column *newcolumn;
    offset += Column::DeserializeFrom(buf + offset, newcolumn);
   columns.push_back(newcolumn);
  }
  schema = new Schema(columns, 1);
  return offset;
}