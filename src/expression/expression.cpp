#include "expression/expression.h"

namespace mortred {
namespace expression {

void UnaryExpression::Resolve(std::shared_ptr<Schema> schema) {
  child_->Resolve(schema);
  nullable_ = child_->nullable();
}
void BinaryExpression::Resolve(std::shared_ptr<Schema> schema) {
  left_->Resolve(schema);
  right_->Resolve(schema);
  nullable_ = left_->nullable() || right_->nullable();
}
void TenaryExpression::Resolve(std::shared_ptr<Schema> schema) {
  child1_->Resolve(schema);
  child2_->Resolve(schema);
  child3_->Resolve(schema);
  nullable_ = child1_->nullable() || child2_->nullable() || child3_->nullable();
}
void ArrayExpression::Resolve(std::shared_ptr<Schema> schema) {
  nullable_ = false;
  for (auto& child : children_) {
    child->Resolve(schema);
    nullable_ = nullable_ || child->nullable();
  }
}
} //namespace expression
} //namespace mortred
