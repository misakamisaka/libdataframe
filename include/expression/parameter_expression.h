#ifndef MORTRED_PARAMETER_EXPRESSION_H
#define MORTRED_PARAMETER_EXPRESSION_H

#include "expression.h"

namespace mortred {
namespace expression {

class Parameter : public LeafExpression {
 public:
  virtual void Resolve(std::shared_ptr<Schema> schema);
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row>);
 private:
  size_t index_;
};

class Constant : public LeafExpression {
 public:
  virtual void Resolve(std::shared_ptr<Schema> schema);
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row>);
};

} //namespace expression
} //namespace mortred

#endif //MORTRED_PARAMETER_EXPRESSION_H
