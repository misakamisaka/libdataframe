#include "expression/arithmetic_expression.h"

using std::make_shared;

namespace mortred {
namespace expression {
void UnaryMinus::Resolve(std::shared_ptr<Schema> schema) {
  UnaryExpression::Resolve(schema);
  data_type_ = child_->data_type();
}
std::shared_ptr<DataField> UnaryMinus::Eval(std::shared_ptr<Row> row) {
  std::shared_ptr<DataField> child_data_field = child_->Eval(row);
  std::shared_ptr<DataField> ret = make_shared<DataField>();
  ret->cell = make_shared<Cell>();
  ret->cell->is_null = child_data_field->cell->is_null;
  ret->data_type = data_type_;
  if (ret->cell->is_null) {
    return ret;
  }
#define PRIMITIVE_CASE(FIELD_TYPE, C_TYPE)                                       \
  case FIELD_TYPE: {                                                             \
    ret->cell->value = -boost::any_cast<C_TYPE>(child_data_field->cell->value);  \
    break;                                                                       \
  }

  switch (data_type_->type) {
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
    default:
      throw ExpressionException("unsupported Arithmetic, type[ " +
          std::to_string(static_cast<typename std::underlying_type<Type>::type>(data_type_->type)) + "]");
  }

#undef PRIMITIVE_CASE

  return ret;
}

void Abs::Resolve(std::shared_ptr<Schema> schema) {
  UnaryExpression::Resolve(schema);
  data_type_ = child_->data_type();
}

std::shared_ptr<DataField> Abs::Eval(std::shared_ptr<Row> row) {
  std::shared_ptr<DataField> child_data_field = child_->Eval(row);
  std::shared_ptr<DataField> ret = make_shared<DataField>();
  ret->cell = make_shared<Cell>();
  ret->cell->is_null = child_data_field->cell->is_null;
  ret->data_type = data_type_;
  if (ret->cell->is_null) {
    return ret;
  }
#define PRIMITIVE_CASE(FIELD_TYPE, C_TYPE)                                       \
  case FIELD_TYPE: {                                                             \
    C_TYPE tmp_value = boost::any_cast<C_TYPE>(child_data_field->cell->value);   \
    ret->cell->value = (tmp_value >= 0)? tmp_value : -tmp_value;                 \
    break;                                                                       \
  }

  switch (data_type_->type) {
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
    default:
      throw ExpressionException("unsupported Arithmetic, type[ " +
          std::to_string(static_cast<typename std::underlying_type<Type>::type>(data_type_->type)) + "]");
  }

#undef PRIMITIVE_CASE

  return ret;
}

void ArrayExpressionWithInputTypeCheck::Resolve(std::shared_ptr<Schema> schema) {
  ArrayExpression::Resolve(schema);
  data_type_ = children_[0]->data_type();
  for (auto& child: children_) {
    if (!data_type_->Equals(child->data_type())) {
      throw ExpressionException("children of ArrayExpressionWithInputTypeCheck must have same type");
    }
  }
}

std::shared_ptr<DataField> Least::Eval(std::shared_ptr<Row> row) {
  std::vector<std::shared_ptr<DataField>> data_fields;
  for (auto& child : children_) {
    data_fields.push_back(child->Eval(row));
  }

  std::shared_ptr<DataField> ret = make_shared<DataField>();
  ret->cell = make_shared<Cell>();
  ret->cell->is_null = std::any_of(data_fields.begin(), data_fields.end(), [](const std::shared_ptr<DataField> data_field) { return data_field->cell->is_null; });
  ret->data_type = data_type_;
  if (ret->cell->is_null) {
    return ret;
  }

#define PRIMITIVE_CASE(FIELD_TYPE, C_TYPE)                         \
  case FIELD_TYPE: {                                               \
    C_TYPE tmp_value = std::numeric_limits<C_TYPE>::max();         \
    for (auto& data_field: data_fields) {                          \
      tmp_value = std::min<C_TYPE>(tmp_value,                      \
          boost::any_cast<C_TYPE>(data_field->cell->value)); \
    }                                                              \
    ret->cell->value = tmp_value;                                  \
    break;                                                         \
  }

  switch (data_type_->type) {
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
    default:
      throw ExpressionException("unsupported Arithmetic, type[ " +
          std::to_string(static_cast<typename std::underlying_type<Type>::type>(data_type_->type)) + "]");
  }

#undef PRIMITIVE_CASE

  return ret;
}

std::shared_ptr<DataField> Greatest::Eval(std::shared_ptr<Row> row) {
  std::vector<std::shared_ptr<DataField>> data_fields;
  for (auto& child : children_) {
    data_fields.push_back(child->Eval(row));
  }
  std::shared_ptr<DataField> ret = make_shared<DataField>();

  ret->cell = make_shared<Cell>();
  ret->cell->is_null = std::any_of(data_fields.begin(), data_fields.end(), [](const std::shared_ptr<DataField> data_field) { return data_field->cell->is_null; });
  ret->data_type = data_type_;
  if (ret->cell->is_null) {
    return ret;
  }

#define PRIMITIVE_CASE(FIELD_TYPE, C_TYPE)                         \
  case FIELD_TYPE: {                                               \
    C_TYPE tmp_value = std::numeric_limits<C_TYPE>::min();         \
    for (auto& data_field: data_fields) {                          \
      tmp_value = std::max<C_TYPE>(tmp_value,                      \
          boost::any_cast<C_TYPE>(data_field->cell->value));       \
    }                                                              \
    ret->cell->value = tmp_value;                                  \
    break;                                                         \
  }

  switch (data_type_->type) {
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
    default:
      throw ExpressionException("unsupported Arithmetic, type[ " +
          std::to_string(static_cast<typename std::underlying_type<Type>::type>(data_type_->type)) + "]");
  }

#undef PRIMITIVE_CASE

  return ret;
}
}
}
