#ifndef MORTRED_BITWISE_EXPRESSION_H
#define MORTRED_BITWISE_EXPRESSION_H

#include "expression.h"

namespace mortred {
namespace expression {

class BitwiseNot : public UnaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row>);
};

class BitwiseAnd : public BinaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row>);
};

class BitwiseOr : public BinaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row>);
};

class BitwiseXor : public BinaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row>);
};

} //namespace expression
} //namespace mortred

#endif //MORTRED_BITWISE_EXPRESSION_H
