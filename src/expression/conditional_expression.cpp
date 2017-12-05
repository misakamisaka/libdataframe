#include "expression/conditional_expression.h"
#include <glog/logging.h>
#include "column.h"
#include "expression/expression_exception.h"
#include "row.h"
#include "schema.h"

namespace mortred {
namespace expression{

void If::Resolve(const std::shared_ptr<Schema>& schema) {
  TenaryExpression::Resolve(schema);
  if (child1_->data_type()->type != Type::BOOL) {
      throw ExpressionException("IfExpression: type of child1 must be BOOL");
  } else if (!child2_->data_type()->Equals(child3_->data_type())) {
      throw ExpressionException("IfExpression: type of child2 and child3 must be same");
  }
}
std::shared_ptr<DataField> If::Eval(const std::shared_ptr<Row>& row) const {
  std::shared_ptr<DataField> data_field1 = child1_->Eval(row);
  std::shared_ptr<DataField> data_field2 = child2_->Eval(row);
  std::shared_ptr<DataField> data_field3 = child3_->Eval(row);
  boost::any value;
  if (data_field1->cell()->is_null()) {
    return data_field3;
  } else if ((boost::any_cast<bool>(data_field1->cell()->value()))) {
    return data_field2;
  } else {
    return data_field3;
  }
}

void CaseWhen::Resolve(const std::shared_ptr<Schema>& schema) {
  if (branches_.empty()) {
    throw ExpressionException("CaseWhenExpression: no branch defined");
  }
  for (size_t i = 0; i < branches_.size(); ++i) {
    branches_[i].first->Resolve(schema);
    branches_[i].second->Resolve(schema);
    if (branches_[i].first->data_type()->type != Type::BOOL) {
      throw ExpressionException("CaseWhenExpression: first data types of branches are not BOOL");
    }
    if (i == 0) {
      data_type_ = branches_[i].second->data_type();
    } else if (!branches_[i].second->data_type()->Equals(data_type_)){
      throw ExpressionException("CaseWhenExpression: second data types of branches are not same");
    }
    
  }
  else_value_->Resolve(schema);
  if (!else_value_->data_type()->Equals(data_type_)){
    throw ExpressionException("CaseWhenExpression: data types of else are not same");
  }
  
  nullable_ = true;
  resolved_ = true;
}
std::shared_ptr<DataField> CaseWhen::Eval(const std::shared_ptr<Row>& row) const {
  for (auto& branch : branches_) {
    std::shared_ptr<DataField> data_field1 = branch.first->Eval(row);
    std::shared_ptr<DataField> data_field2 = branch.second->Eval(row);
    if ((boost::any_cast<bool>(data_field1->cell()->value()))) {
      return data_field2;
    }
  }
  return else_value_->Eval(row);
}
std::string CaseWhen::ToString() {
  std::string ret = "case ";
  for (auto& branch : branches_) {
    ret += "when ";
    ret += branch.first->ToString();
    ret += "then ";
    ret += branch.second->ToString();
    ret += " ";
  }
  ret += "else ";
  ret += else_value_->ToString();
  return ret;
}
} //namespace mortred
} //namespace expression
