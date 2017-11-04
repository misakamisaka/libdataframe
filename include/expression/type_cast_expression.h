#ifndef MORTRED_TYPE_CAST_EXPRESSION_H
#define MORTRED_TYPE_CAST_EXPRESSION_H

#include "expression.h"

namespace mortred {
namespace expression {

class TypeCastExpr : public UnaryExpression {
 public:
  TypeCastExpr(std::shared_ptr<Expression> child,
      const std::string& column_name,
      const std::shared_ptr<DataType>& data_type)
    :UnaryExpression(child, NodeType::CONVERT),
    column_name_(column_name) { 
      data_type_ = data_type;
    }
  virtual void Resolve(const std::shared_ptr<Schema>& schema);
  virtual std::shared_ptr<DataField> Eval(const std::shared_ptr<Row>& row) const;
  virtual std::string ToString() {
    return child_->ToString() + "AS" + column_name_;
  }
  std::string column_name() const { return column_name_; }
 private:
  std::string column_name_;
};

} //namespace expression
} //namespace mortred
#endif //MORTRED_TYPE_CAST_EXPRESSION_H
