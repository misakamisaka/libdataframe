#ifndef MORTRED_PREDICATE_EXPRESSION_H
#define MORTRED_PREDICATE_EXPRESSION_H

#include "expression.h"

namespace mortred {
namespace expression {

class Not : public UnaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Schema> schema, std::shared_ptr<Row>);
};

class AndAlso : public BinaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Schema> schema, std::shared_ptr<Row>);
};

class OrElse : public BinaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Schema> schema, std::shared_ptr<Row>);
};

class LessThan : public BinaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Schema> schema, std::shared_ptr<Row>);
};

class LessThanOrEqual : public BinaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Schema> schema, std::shared_ptr<Row>);
};

class GreaterThan : public BinaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Schema> schema, std::shared_ptr<Row>);
};

class GreaterThanOrEqual : public BinaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Schema> schema, std::shared_ptr<Row>);
};

class EqualTo : public BinaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Schema> schema, std::shared_ptr<Row>);
};

class NotEqualTo : public BinaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Schema> schema, std::shared_ptr<Row>);
};

class In : public BinaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Schema> schema, std::shared_ptr<Row>);
};

class NotIn : public BinaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Schema> schema, std::shared_ptr<Row>);
};

} //namespace expression
} //namespace mortred
#endif //MORTRED_PREDICATE_EXPRESSION_H
