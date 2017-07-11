#ifndef MORTRED_ALIAS_EXPRESSION_H
#define MORTRED_ALIAS_EXPRESSION_H

#include "expression.h"

namespace mortred {
namespace expression {

class Column : public LeafExpression {
 public:
  virtual void Resolve(std::shared_ptr<Schema> schema);
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row);
  virtual std::string ToString() {
    return "$" + column_name_;
  }
 private:
  std::string column_name_;
};

class Alias : public UnaryExpression {
 public:
  virtual void Resolve(std::shared_ptr<Schema> schema);
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row);
  virtual std::string ToString() {
    return child_->ToString() + "AS" + alias_name_;
  }
 private:
  std::string alias_name_;
};

} //namespace expression
} //namespace mortred
#endif //MORTRED_ALIAS_EXPRESSION_H
