#ifndef MORTRED_PREDICATE_EXPRESSION_H
#define MORTRED_PREDICATE_EXPRESSION_H

#include "expression.h"
#include "type/type.h"
#include "type/type_converter.h"
#include <glog/logging.h>

namespace mortred {
namespace expression {

class Not : public UnaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(const std::shared_ptr<Row>&) const;
};

template<template<typename T> class PredicateMethod, NodeType NodeTypeId, typename BinaryPredicateResolvePolicy>
class BinaryPredicate : public BinaryExpression, public BinaryPredicateResolvePolicy {
 public:
   BinaryPredicate(std::shared_ptr<Expression> left,
       std::shared_ptr<Expression> right)
     : BinaryExpression(left, right, NodeTypeId) { }
  virtual void Resolve(const std::shared_ptr<Schema>& schema) {
    BinaryExpression::Resolve(schema);
    BinaryPredicateResolvePolicy::Resolve(schema, left_, right_);
  }
  virtual std::shared_ptr<DataField> Eval(const std::shared_ptr<Row>& row) const;
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

class LogicalPredicateResolvePolicy {
 public:
  virtual void Resolve(std::shared_ptr<Schema> schema, std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);
};

class ComparisonPredicateResolvePolicy {
 public:
  virtual void Resolve(std::shared_ptr<Schema> schema, std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);
};

class AndAlso : public BinaryPredicate<std::logical_and, NodeType::AND_ALSO, LogicalPredicateResolvePolicy> {
 public:
  using BinaryPredicate::BinaryPredicate;
};

class OrElse : public BinaryPredicate<std::logical_or, NodeType::OR_ELSE, LogicalPredicateResolvePolicy> {
 public:
  using BinaryPredicate::BinaryPredicate;
};

class LessThan : public BinaryPredicate<std::less, NodeType::LT, ComparisonPredicateResolvePolicy> {
 public:
  using BinaryPredicate::BinaryPredicate;
};

class LessThanOrEqual : public BinaryPredicate<std::less_equal, NodeType::LE, ComparisonPredicateResolvePolicy> {
 public:
  using BinaryPredicate::BinaryPredicate;
};

class GreaterThan : public BinaryPredicate<std::greater, NodeType::GT, ComparisonPredicateResolvePolicy> {
 public:
  using BinaryPredicate::BinaryPredicate;
};

class GreaterThanOrEqual : public BinaryPredicate<std::greater_equal, NodeType::GE, ComparisonPredicateResolvePolicy> {
 public:
  using BinaryPredicate::BinaryPredicate;
};

class EqualTo : public BinaryPredicate<std::equal_to, NodeType::EQ, ComparisonPredicateResolvePolicy> {
 public:
  using BinaryPredicate::BinaryPredicate;
};

class NotEqualTo : public BinaryPredicate<std::not_equal_to, NodeType::NE, ComparisonPredicateResolvePolicy> {
 public:
  using BinaryPredicate::BinaryPredicate;
};

class In : public BinaryExpression {
 public:
  virtual void Resolve(const std::shared_ptr<Schema>& schema);
  virtual std::shared_ptr<DataField> Eval(const std::shared_ptr<Row>&) const;
  virtual std::string ToString() {
  }
};

class NotIn : public BinaryExpression {
 public:
  virtual void Resolve(const std::shared_ptr<Schema>& schema);
  virtual std::shared_ptr<DataField> Eval(const std::shared_ptr<Row>&) const;
  virtual std::string ToString() {
  }
};

template<template<typename T> class PredicateMethod, NodeType NodeTypeId, typename BinaryPredicateResolvePolicy> 
std::shared_ptr<DataField> BinaryPredicate<PredicateMethod, NodeTypeId, BinaryPredicateResolvePolicy>::Eval(const std::shared_ptr<Row>& row) const {
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
  std::shared_ptr<DataType> common_type = DataTypes::FindTightesetCommonType(left_->data_type(), right_->data_type());
  left_data_field = type_cast(common_type, left_data_field);
  right_data_field = type_cast(common_type, right_data_field);

#define PRIMITIVE_CASE(FIELD_TYPE, C_TYPE)                                         \
  case FIELD_TYPE: {                                                               \
    C_TYPE left_value = boost::any_cast<C_TYPE>(left_data_field->cell->value());   \
    C_TYPE right_value = boost::any_cast<C_TYPE>(right_data_field->cell->value()); \
    value = PredicateMethod<C_TYPE>{}(left_value, right_value);                    \
    break;                                                                         \
  }

  switch (common_type->type) {
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
#endif //MORTRED_PREDICATE_EXPRESSION_H
