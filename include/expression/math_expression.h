#ifndef MORTRED_MATH_EXPRESSION_H
#define MORTRED_MATH_EXPRESSION_H

#include "expression.h"

namespace mortred {
namespace expression {

class Floor : public UnaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row>);
};

class Cell : public UnaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row>);
};

class Round : public UnaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row>);
};

class Power : public BinaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row>);
};

class Sqrt : public BinaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row>);
};

class LeftShift : public BinaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row>);
};

class RightShift : public BinaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row>);
};

} //namespace expression
} //namespace mortred
#endif //MORTRED_PREDICATE_EXPRESSION_H
