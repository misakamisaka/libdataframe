#include "expression/predicate_expression.h"
#include "type/type.h"

namespace mortred {
namespace expression {
std::shared_ptr<DataField> Not::Eval(std::shared_ptr<Row>) {
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

void In::Resolve(std::shared_ptr<Schema> schema) {
}
std::shared_ptr<DataField> In::Eval(std::shared_ptr<Row>) {
}

void NotIn::Resolve(std::shared_ptr<Schema> schema) {
}
std::shared_ptr<DataField> NotIn::Eval(std::shared_ptr<Row>) {
}

}
}