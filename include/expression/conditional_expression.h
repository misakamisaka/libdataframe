#ifndef MORTRED_CONDITIONAL_EXPRESSION_H
#define MORTRED_CONDITIONAL_EXPRESSION_H

#include "expression.h"

namespace mortred {
namespace expression {

class If : public TenaryExpression {
 public:
  If(std::shared_ptr<Expression> child1,
       std::shared_ptr<Expression> child2,
       std::shared_ptr<Expression> child3)
    :TenaryExpression(child1, child2, child3, NodeType::IF) { }
  virtual void Resolve(const std::shared_ptr<Schema>& schema);
  virtual std::shared_ptr<DataField> Eval(const std::shared_ptr<Row>& row) const;
  virtual std::string ToString() {
    return "if(" + child1_->ToString() + "," + child2_->ToString() + "," + child3_->ToString() + ")";
  }
};

class CaseWhen : public Expression {
  using Branch = std::pair<std::shared_ptr<Expression>, std::shared_ptr<Expression>>;
 public:
  CaseWhen(std::vector<Branch> branches, std::shared_ptr<Expression> else_value)
  :Expression(NodeType::CASE_WHEN), branches_(branches), else_value_(else_value) { }
  virtual void Resolve(const std::shared_ptr<Schema>& schema);
  virtual std::shared_ptr<DataField> Eval(const std::shared_ptr<Row>& row) const;
  virtual std::string ToString();
 protected:
  std::vector<Branch> branches_;
  std::shared_ptr<Expression> else_value_;
};

} //namespace expression
} //namespace mortred

#endif //MORTRED_CONDITIONAL_EXPRESSION_H
