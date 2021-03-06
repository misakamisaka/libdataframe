#include "data_field.h"
#include "data_field_exception.h"
#include "type/type.h"
#include "type/type_converter.h"

namespace mortred {

template<template<typename T> class PredicateMethod>
inline bool data_field_compare(const DataField& left_data_field,
    const DataField& right_data_field) {
  if (!left_data_field.data_type()->Equals(right_data_field.data_type())) {
      throw DataFieldException("types are not compatible, left_type[ " +
          std::to_string(static_cast<typename std::underlying_type<Type>::type>(left_data_field.data_type()->type)) + "],right_type[" +
          std::to_string(static_cast<typename std::underlying_type<Type>::type>(right_data_field.data_type()->type)) + "]");
  }
#define PRIMITIVE_CASE(FIELD_TYPE, C_TYPE)                                         \
  case FIELD_TYPE: {                                                               \
    C_TYPE left_value = boost::any_cast<C_TYPE>(left_data_field.cell()->value());    \
    C_TYPE right_value = boost::any_cast<C_TYPE>(right_data_field.cell()->value());  \
    return PredicateMethod<C_TYPE>{}(left_value, right_value);                     \
  }

  switch (left_data_field.data_type()->type) {
    PRIMITIVE_CASE(Type::UINT8, uint8_t)
    PRIMITIVE_CASE(Type::INT8, int8_t)
    PRIMITIVE_CASE(Type::UINT16, uint16_t)
    PRIMITIVE_CASE(Type::INT16, int16_t)
    PRIMITIVE_CASE(Type::UINT32, uint32_t)
    PRIMITIVE_CASE(Type::INT32, int32_t)
    PRIMITIVE_CASE(Type::UINT64, uint64_t)
    PRIMITIVE_CASE(Type::INT64, int64_t)
    PRIMITIVE_CASE(Type::FLOAT, float)
    PRIMITIVE_CASE(Type::DOUBLE, double)
    PRIMITIVE_CASE(Type::STRING, std::string)
    default:
      throw DataFieldException("unsupported data field compare, type[ " +
          std::to_string(static_cast<typename std::underlying_type<Type>::type>(left_data_field.data_type()->type)) + "]");
  }

#undef PRIMITIVE_CASE
  return false;
}

DataField::DataField(const std::string& value) {
  data_type_ = DataTypes::MakeStringType();
  cell_ = std::make_shared<Cell>(false, value);
}

std::string DataField::ToString() const {
  StringConverter str_converter(std::static_pointer_cast<StringType>(DataTypes::MakeStringType()));
  std::shared_ptr<DataField> data_field = str_converter.ConvertFrom(*this);
  if(data_field->cell_->is_null()) {
    return "NULL";
  } else {
    return boost::any_cast<std::string>(data_field->cell_->value());
  }
}

bool DataField::LessThan(const std::shared_ptr<DataField>& data_field) {
  return LessThan(*data_field);
}
bool DataField::GreaterThan(const std::shared_ptr<DataField>& data_field) {
  return GreaterThan(*data_field);
}
bool DataField::Equal(const std::shared_ptr<DataField>& data_field) {
  return Equal(*data_field);
}
bool DataField::LessThan(const DataField& data_field) {
  if (cell_->is_null() && data_field.cell_->is_null()) {
    return false;
  } else if (cell_->is_null()) {
    return true;
  } else if (data_field.cell_->is_null()) {
    return false;
  }
  return data_field_compare<std::less>(*this, data_field);
}
bool DataField::GreaterThan(const DataField& data_field) {
  if (cell_->is_null() && data_field.cell_->is_null()) {
    return false;
  } else if (cell_->is_null()) {
    return false;
  } else if (data_field.cell_->is_null()) {
    return true;
  }
  return data_field_compare<std::greater>(*this, data_field);
}
bool DataField::Equal(const DataField& data_field) {
  if (cell_->is_null() && data_field.cell_->is_null()) {
    return true;
  } else if (cell_->is_null()) {
    return false;
  } else if (data_field.cell_->is_null()) {
    return false;
  }
  return data_field_compare<std::equal_to>(*this, data_field);
}
}
