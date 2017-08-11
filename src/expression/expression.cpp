#include "expression/expression.h"

namespace mortred {
namespace expression {

void LeafExpression::Resolve(std::shared_ptr<Schema>) {
  nullable_ = true;
  resolved_ = true;
}
//std::shared_ptr<DataField> LeafExpression::Eval(std::shared_ptr<Row> row) {
//      throw ExpressionException("LeafExpression does not support Eval");
//}
//std::string LeafExpression::ToString() {
// return "leaf";
//}
void UnaryExpression::Resolve(std::shared_ptr<Schema> schema) {
  child_->Resolve(schema);
  nullable_ = child_->nullable();
  data_type_ = child_->data_type();
  resolved_ = true;
}
//std::shared_ptr<DataField> UnaryExpression::Eval(std::shared_ptr<Row> row) {
//      throw ExpressionException("UnaryExpression does not support Eval");
//}
//std::string UnaryExpression::ToString() {
//  return "unary(" + child_->ToString() + ")";
//}
void BinaryExpression::Resolve(std::shared_ptr<Schema> schema) {
  left_->Resolve(schema);
  right_->Resolve(schema);
  nullable_ = left_->nullable() || right_->nullable();
  resolved_ = true;
}
//std::shared_ptr<DataField> BinaryExpression::Eval(std::shared_ptr<Row> row) {
//      throw ExpressionException("BinaryExpression does not support Eval");
//}
//std::string BinaryExpression::ToString() {
//  return "binary(" + left_->ToString() + ", " + right_->ToString() + ")";
//}
void TenaryExpression::Resolve(std::shared_ptr<Schema> schema) {
  child1_->Resolve(schema);
  child2_->Resolve(schema);
  child3_->Resolve(schema);
  nullable_ = child1_->nullable() || child2_->nullable() || child3_->nullable();
  resolved_ = true;
}
//std::shared_ptr<DataField> TenaryExpression::Eval(std::shared_ptr<Row> row) {
//      throw ExpressionException("TenaryExpression does not support Eval");
//}
//std::string TenaryExpression::ToString() {
//  return "tenary(" + child1_->ToString() + ", " + child2_->ToString() + ", " + child3_->ToString() + ")";
//}
void ArrayExpression::Resolve(std::shared_ptr<Schema> schema) {
  nullable_ = false;
  for (auto& child : children_) {
    child->Resolve(schema);
    nullable_ = nullable_ || child->nullable();
  }
  resolved_ = true;
}

std::shared_ptr<DataField> PairExpression::Eval(std::shared_ptr<Row>) {
      throw ExpressionException("PairExpression does not support Eval");
}
std::string PairExpression::ToString() {
  return "pair(" + left_->ToString() + ", " + right_->ToString() + ")";
}
std::shared_ptr<DataField> ArrayExpression::Eval(std::shared_ptr<Row>) {
      throw ExpressionException("ArrayExpression does not support Eval");
}
std::string ArrayExpression::ToString() {
  std::string ret = "array[";
  for (size_t i = 0; i < children_.size(); ++i) {
    ret += children_[i]->ToString();
    if (i != children_.size() - 1) {
      ret += ", ";
    }
  }
  ret += "]";
  return ret;
}
} //namespace expression
} //namespace mortred
