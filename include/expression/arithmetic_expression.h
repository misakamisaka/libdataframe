#ifndef MORTRED_ARITHMETIC_EXPRESSION_H
#define MORTRED_ARITHMETIC_EXPRESSION_H

#include "expression.h"

namespace mortred {
namespace expression {

class UnaryMinus : public UnaryExpression {
 public:
  virtual void Resolve(std::shared_ptr<Schema> schema);
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row);
  virtual std::string ToString() {
    std::string ret;
    ret += "-(";
    ret += left->ToString();
    ret += ")";
    return ret;
  }
};

class Abs : public UnaryExpression {
 public:
  virtual void Resolve(std::shared_ptr<Schema> schema);
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row);
  virtual std::string ToString() {
    std::string ret;
    ret += "abs(";
    ret += left->ToString();
    ret += ")";
    return ret;
  }
};

template<template<typename T> class ArithmeticMethod, bool NeedCheckDivideByZero>
class BinaryArithmetic : public BinaryExpression {
 protected:
  virtual void Resolve(std::shared_ptr<Schema> schema);
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row);
  virtual void CheckInputDataTypes();
}

class Add : public BinaryArithmetic<std::plus, false> {
 public:
  virtual std::string ToString() {
    std::string ret;
    ret += "(";
    ret += left->ToString();
    ret += ") + (";
    ret += right->ToString();
    ret += ")";
    return ret;
  }
};

class Subtract : public BinaryArithmetic<std::minus, false> {
 public:
  virtual std::string ToString() {
    std::string ret;
    ret += "(";
    ret += left->ToString();
    ret += ") - (";
    ret += right->ToString();
    ret += ")";
    return ret;
  }
};

class Multiply : public BinaryArithmetic<std::multiplies, false> {
 public:
  virtual std::string ToString() {
    std::string ret;
    ret += "(";
    ret += left->ToString();
    ret += ") * (";
    ret += right->ToString();
    ret += ")";
    return ret;
  }
};

class Divide : public BinaryArithmetic<std::divides, true> {
 public:
  virtual std::string ToString() {
    std::string ret;
    ret += "(";
    ret += left->ToString();
    ret += ") / (";
    ret += right->ToString();
    ret += ")";
    return ret;
  }
};

class Modulo : public BinaryArithmetic<std::modulus, true> {
 public:
  virtual std::string ToString() {
    std::string ret;
    ret += "(";
    ret += left->ToString();
    ret += ") % (";
    ret += right->ToString();
    ret += ")";
    return ret;
  }
};

class ArrayExpressionWithInputTypeCheck : public ArrayExpression {
 public:
  virtual void Resolve(std::shared_ptr<Schema> schema);
};

class Least : public ArrayExpressionWithInputTypeCheck {
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

class Greatest : public ArrayExpressionWithInputTypeCheck {
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

template<template<typename T> class ArithmeticMethod, bool NeedCheckDivideByZero>
void BinaryArithmetic<ArithmeticMethod, NeedCheckDivideByZero>::Resolve(
    std::shared_ptr<Schema> schema) {
  BinaryExpression::Resolve();
  data_type_ = DataTypes::FindTightesetCommonType(left_>data_type_, right_->data_type_);
  CheckInputDataTypes();
}

template<template<typename T> class ArithmeticMethod, bool NeedCheckDivideByZero>
std::shared_ptr<DataField> BinaryArithmetic<ArithmeticMethod, NeedCheckDivideByZero>::Eval(
    std::shared_ptr<Row> row) {
  std::shared_ptr<DataField> left_data_field = left_->eval(row);
  std::shared_ptr<DataField> right_data_field = right_->eval(row);
  std::shared_ptr<DataField> ret = make_shared<DataField>();
  ret->cell = make_shared<Cell>();
  ret->cell->is_null = left_data_field->cell->is_null || right_data_field->cell->is_null;
  ret->data_type = data_type_;
  if (ret->cell->is_null) {
    return ret;
  }
  left_data_field = type_cast(data_type_, left_data_field);
  right_data_field = type_cast(data_type_, right_data_field);

#define PRIMITIVE_CASE(FIELD_TYPE, C_TYPE)                                       \
  case FIELD_TYPE: {                                                             \
    C_TYPE left_value = boost::any_cast<C_TYPE>(left_data_field->cell->value);   \
    C_TYPE right_value = boost::any_cast<C_TYPE>(right_data_field->cell->value); \
    if (NeedCheckDivideByZero && right_value == 0) {                             \
      throw ExpressionException("DivideByZero in ArithmeticExpression");         \
    }                                                                            \
    ret->cell->value = ArithmeticMethod<C_TYPE>{}(left_value, right_value);      \
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
          std::to_string(data_type_->type) + "]");
  }

#undef PRIMITIVE_CASE

  return ret;
}

} //namespace expression
} //namespace mortred
#endif //MORTRED_ARITHMETIC_EXPRESSION_H
