#ifndef MORTRED_ALIAS_EXPRESSION_H
#define MORTRED_ALIAS_EXPRESSION_H

#include "expression.h"

namespace mortred {
namespace expression {

class ColumnExpr : public LeafExpression {
 public:
  ColumnExpr(const std::string column_name)
    :LeafExpression(NodeType::COLUMN),
    column_name_(column_name) { }
  virtual void Resolve(std::shared_ptr<Schema> schema);
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row);
  virtual std::string ToString() {
    return "$" + column_name_;
  }
  std::string column_name() const { return column_name_; }
 private:
  std::string column_name_;
  int index_;
};

class AliasExpr : public UnaryExpression {
 public:
  AliasExpr(std::shared_ptr<Expression> child, const std::string alias_name)
    :UnaryExpression(child, NodeType::COLUMN),
    alias_name_(alias_name) { }
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row);
  virtual std::string ToString() {
    return child_->ToString() + "AS" + alias_name_;
  }
  std::string alias_name() const { return alias_name_; }
 private:
  std::string alias_name_;
};

class ConstantExpr : public LeafExpression {
 public:
  ConstantExpr(bool is_null, const std::string value_str, std::shared_ptr<DataType> data_type)
    :LeafExpression(NodeType::CONSTANT),
    is_null_(is_null),
    value_str_(value_str) {
    data_type_ = data_type;
    nullable_ = is_null;
  }
  virtual void Resolve(std::shared_ptr<Schema> schema);
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row>);
  virtual std::string ToString() {
    return value_str_;
  }
 private:
  bool is_null_;
  std::string value_str_;
};

} //namespace expression
} //namespace mortred
#endif //MORTRED_ALIAS_EXPRESSION_H
