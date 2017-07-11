#ifndef MORTRED_CONDITIONAL_EXPRESSION_H
#define MORTRED_CONDITIONAL_EXPRESSION_H

#include "expression.h"

namespace mortred {
namespace expression {

class Conditional : public tenaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Schema> schema, std::shared_ptr<Row>);
};

class CaseWhen : public BinaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Schema> schema, std::shared_ptr<Row>);
};

} //namespace expression
} //namespace mortred

#endif //MORTRED_CONDITIONAL_EXPRESSION_H
