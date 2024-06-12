#include "record/row.h"

/**
与元组的概念等价，用于存储记录或索引键，一个Row由一个或多个Field构成。
 */
//实现原理：将Row对象序列化到buf中

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");

  uint32_t offset = 0;
  //写入magic number
  uint32_t field_count = fields_.size();
  memcpy(buf + offset, &field_count, sizeof(field_count));
  offset += sizeof(field_count);//写入field的数量

  char * field_buf = buf + offset;
  uint32_t field_offset = (field_count + 7)/8;//这个是用来存储哪些field是null的
  memset(field_buf, 0, field_offset);//初始化为0，表示所有的field都不是null
  offset += field_offset;

  for(uint32_t i=0; i<field_count; i++){
    if(fields_[i]==nullptr){
      field_buf[i/8] |= (1 << (i%8));//如果是null的话，将对应的位置为1
    }
    else {
      offset += fields_[i]->SerializeTo(buf + offset);
    }
   
}

  return offset;
}
uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");//只有空的row可以用这个方法解析
  uint32_t offset = 0;
  uint32_t field_count;
  memcpy(&field_count, buf + offset, sizeof(field_count));//读取field的数量
  offset += sizeof(field_count);

  char * field_buf = buf + offset;
  uint32_t field_offset = (field_count + 7)/8;
  offset += field_offset;
  fields_.clear();fields_.resize(field_count, nullptr);
  for(uint32_t i=0; i<field_count; i++){
    if(field_buf[i/8] & (1 << (i%8)))//
    {
      offset += Field::DeserializeFrom(offset+buf,schema->GetColumns()[i]->GetType(),&fields_[i], true);
    }
    else {
      offset += Field::DeserializeFrom(offset+buf,schema->GetColumns()[i]->GetType(),&fields_[i], false);
    }
  }
  return offset;
}
//实现原理：获取Row对象序列化后的大小,单位是字节
uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t size = 0;
  uint32_t field_count = fields_.size();
  size += sizeof(field_count);
  size += (field_count + 7) / 8;
  for (uint32_t i = 0; i < field_count; i++) {
    if (fields_[i] != nullptr)size += fields_[i]->GetSerializedSize();
  }
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
