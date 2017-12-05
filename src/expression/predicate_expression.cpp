#include "expression/predicate_expression.h"
#include "type/type.h"

namespace mortred {
namespace expression {
void Not::Resolve(const std::shared_ptr<Schema>& schema) {
  UnaryExpression::Resolve(schema);
  if (child_->data_type()->type != Type::BOOL) {
      throw ExpressionException("type of child of Not must be BOOL");
  }
}
std::shared_ptr<DataField> Not::Eval(const std::shared_ptr<Row>& row) const {
  std::shared_ptr<DataField> data_field = child_->Eval(row);
  boost::any value;
  if (data_field->cell()->is_null()) {
    return std::make_shared<DataField>(std::make_shared<Cell>(true, value), data_type_);
  }
  value = !(boost::any_cast<bool>(data_field->cell()->value()));
  return std::make_shared<DataField>(std::make_shared<Cell>(false, value), data_type_);
}

void IsNull::Resolve(const std::shared_ptr<Schema>& schema) {
  UnaryExpression::Resolve(schema);
  data_type_ = DataTypes::MakeBooleanType();
  nullable_ = false;
}
std::shared_ptr<DataField> IsNull::Eval(const std::shared_ptr<Row>& row) const {
  std::shared_ptr<DataField> data_field = child_->Eval(row);
  if (data_field->cell()->is_null()) {
    return std::make_shared<DataField>(std::make_shared<Cell>(false, true), data_type_);
  } else {
    return std::make_shared<DataField>(std::make_shared<Cell>(false, false), data_type_);
  }
}

void LogicalPredicateResolvePolicy::Resolve(
    std::shared_ptr<Schema> schema,
    std::shared_ptr<Expression> left,
    std::shared_ptr<Expression> right) {
  if (left->data_type()->type != Type::BOOL || right->data_type()->type != Type::BOOL) {
      throw ExpressionException("type of children of LogicalPredicate must be BOOL");
  }
}

void ComparisonPredicateResolvePolicy::Resolve(
    std::shared_ptr<Schema> schema,
    std::shared_ptr<Expression> left,
    std::shared_ptr<Expression> right) {
  TypePtr common_type = DataTypes::FindTightesetCommonType(left->data_type(), right->data_type());
  if (common_type->Equals(DataTypes::MakeNullType())) {
      throw ExpressionException("type of children of LogicalPredicate must be compatible");
  }
}

void In::Resolve(const std::shared_ptr<Schema>& schema) {
}
std::shared_ptr<DataField> In::Eval(const std::shared_ptr<Row>&) const {
}

void NotIn::Resolve(const std::shared_ptr<Schema>& schema) {
}
std::shared_ptr<DataField> NotIn::Eval(const std::shared_ptr<Row>&) const {
}

}
}
