#include "record/column.h"

#include "glog/logging.h"
//column是一列的信息，可以理解为一张表的表头
Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* DONE: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t offset = 0;
  //写入magic number
  uint32_t magic_num = COLUMN_MAGIC_NUM;
  memcpy(buf + offset, &magic_num, sizeof(magic_num));
  offset += sizeof(magic_num);
  //写入column的名字
  uint32_t name_len = name_.size();
  memcpy(buf+offset,&name_len,sizeof(name_len));
  offset += sizeof(name_len);
  memcpy(buf+offset,name_.c_str(),name_len);//name_len是字节数
  offset += name_len;
  //写入type
  memcpy(buf+offset,&type_,sizeof(type_));
  offset += sizeof(type_);
  memcpy(buf+offset,&len_,sizeof(len_));
  offset += sizeof(len_);
  memcpy(buf+offset,&table_ind_,sizeof(table_ind_));
  offset += sizeof(table_ind_);
  memcpy(buf+offset,&nullable_,sizeof(nullable_));
  offset += sizeof(nullable_);
  memcpy(buf+offset,&unique_,sizeof(unique_));
  offset += sizeof(unique_);
  return offset;
}

/**
 * DONE: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  uint32_t size_column = sizeof(COLUMN_MAGIC_NUM) + sizeof(uint32_t) + name_.size() + sizeof(type_) + sizeof(len_) + sizeof(table_ind_) + sizeof(nullable_) + sizeof(unique_);
  return size_column;
}

/**
 * DONE: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  uint32_t offset = 0;
  //写入magic number
  uint32_t magic_num;
  memcpy(&magic_num,buf+offset,sizeof(magic_num));
  ASSERT(magic_num == COLUMN_MAGIC_NUM, "Wrong magic number.");
  offset += sizeof(magic_num);
  //写入column的名字
  uint32_t name_len;
  memcpy(&name_len,buf+offset,sizeof(name_len));
  offset += sizeof(name_len);
  std::string name;
  name.resize(name_len);
  memcpy(&name[0],buf+offset,name_len);
  offset += name_len;
  //写入type
  TypeId type;
  memcpy(&type,buf+offset,sizeof(type));
  offset += sizeof(type);
  //写入len
  uint32_t len;
  memcpy(&len,buf+offset,sizeof(len));
  offset += sizeof(len);
  //写入table_ind
  uint32_t table_ind;
  memcpy(&table_ind,buf+offset,sizeof(table_ind));
  offset += sizeof(table_ind);
  //写入nullable
  bool nullable;
  memcpy(&nullable,buf+offset,sizeof(nullable));
  offset += sizeof(nullable);
  //写入unique
  bool unique;
  memcpy(&unique,buf+offset,sizeof(unique));
  offset += sizeof(unique);
  
  if(type == TypeId::kTypeChar){
    column = new Column(name,type,len,table_ind,nullable,unique);
  }
  else{
    column = new Column(name,type,table_ind,nullable,unique);
  }
  
  return offset;
}
