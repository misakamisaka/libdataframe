#ifndef MORTRED_ARITHMETIC_EXPRESSION_H
#define MORTRED_ARITHMETIC_EXPRESSION_H

#include "expression.h"
#include "type/type.h"
#include "type/type_converter.h"

namespace mortred {
namespace expression {

class UnaryMinusExpr : public UnaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row);
  virtual std::string ToString() {
    std::string ret;
    ret += "-(";
    ret += child_->ToString();
    ret += ")";
    return ret;
  }
};

class AbsExpr : public UnaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row);
  virtual std::string ToString() {
    std::string ret;
    ret += "abs(";
    ret += child_->ToString();
    ret += ")";
    return ret;
  }
};

template<template<typename T> class ArithmeticMethod, bool need_check_divide_by_zero, NodeType NodeTypeId>
class BinaryArithmetic : public BinaryExpression {
 public:
   BinaryArithmetic(std::shared_ptr<Expression> left,
       std::shared_ptr<Expression> right)
     : BinaryExpression(left, right, NodeTypeId) { }
  virtual void Resolve(std::shared_ptr<Schema> schema);
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row);
  //virtual void CheckInputDataTypes();
  virtual std::string ToString() {
    std::string ret;
    ret += "(";
    ret += left_->ToString();
    ret += ") ";
    ret += node_type2str_map.at(node_type_);
    ret += " (";
    ret += right_->ToString();
    ret += ")";
    return ret;
  }
};

class AddExpr : public BinaryArithmetic<std::plus, false, NodeType::ADD> {
 public:
  using BinaryArithmetic::BinaryArithmetic;
};

class SubtractExpr : public BinaryArithmetic<std::minus, false, NodeType::SUB> {
 public:
  using BinaryArithmetic::BinaryArithmetic;
};

class MultiplyExpr : public BinaryArithmetic<std::multiplies, false, NodeType::MUL> {
 public:
  using BinaryArithmetic::BinaryArithmetic;
};

class DivideExpr : public BinaryArithmetic<std::divides, true, NodeType::DIV> {
 public:
  using BinaryArithmetic::BinaryArithmetic;
};

class ModuloExpr : public BinaryArithmetic<std::modulus, true, NodeType::MOD> {
 public:
  //using BinaryArithmetic::BinaryArithmetic;
};

class ArrayExpressionWithInputTypeCheck : public ArrayExpression {
 public:
  virtual void Resolve(std::shared_ptr<Schema> schema);
};

class LeastExpr : public ArrayExpressionWithInputTypeCheck {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row);
  virtual std::string ToString() {
    std::string ret;
    ret += "least(";
    ret += children_[0]->ToString();
    for (size_t i = 1; i < children_.size(); ++i) {
      ret += ", ";
      ret += children_[i]->ToString();
    }
    ret += ")";
    return ret;
  }
};

class GreatestExpr : public ArrayExpressionWithInputTypeCheck {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row);
  virtual std::string ToString() {
    std::string ret;
    ret += "greatest(";
    ret += children_[0]->ToString();
    for (size_t i = 1; i < children_.size(); ++i) {
      ret += ", ";
      ret += children_[i]->ToString();
    }
    ret += ")";
    return ret;
  }
};

template<template<typename T> class ArithmeticMethod, bool need_check_divide_by_zero, NodeType NodeTypeId>
void BinaryArithmetic<ArithmeticMethod, need_check_divide_by_zero, NodeTypeId>::Resolve(
    std::shared_ptr<Schema> schema) {
  BinaryExpression::Resolve(schema);
  data_type_ = DataTypes::FindTightesetCommonType(left_->data_type(), right_->data_type());
  //CheckInputDataTypes();
}

template<template<typename T> class ArithmeticMethod, bool need_check_divide_by_zero, NodeType NodeTypeId>
std::shared_ptr<DataField> BinaryArithmetic<ArithmeticMethod, need_check_divide_by_zero, NodeTypeId>::Eval(
    std::shared_ptr<Row> row) {
  std::shared_ptr<DataField> left_data_field = left_->Eval(row);
  std::shared_ptr<DataField> right_data_field = right_->Eval(row);
  std::shared_ptr<DataField> ret = std::make_shared<DataField>();
  bool is_null = left_data_field->cell->is_null() || right_data_field->cell->is_null();
  boost::any value;
  ret->data_type = data_type_;
  if (is_null) {
    ret->cell = std::make_shared<Cell>(is_null, value);
    return ret;
  }
  left_data_field = type_cast(data_type_, left_data_field);
  right_data_field = type_cast(data_type_, right_data_field);

#define PRIMITIVE_CASE(FIELD_TYPE, C_TYPE)                                       \
  case FIELD_TYPE: {                                                             \
    C_TYPE left_value = boost::any_cast<C_TYPE>(left_data_field->cell->value());   \
    C_TYPE right_value = boost::any_cast<C_TYPE>(right_data_field->cell->value()); \
    if (need_check_divide_by_zero && right_value == 0) {                             \
      throw ExpressionException("DivideByZero in ArithmeticExpression");         \
    }                                                                            \
    value = ArithmeticMethod<C_TYPE>{}(left_value, right_value);      \
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
  ret->cell = std::make_shared<Cell>(is_null, value);
  return ret;
}

} //namespace expression
} //namespace mortred
#endif //MORTRED_ARITHMETIC_EXPRESSION_H
