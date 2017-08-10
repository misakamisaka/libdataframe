#include "data_frame.h"
#include <boost/any.hpp>
#include "expression/expression.h"
#include "data_field.h"

namespace mortred {

using expression::Expression;

//ArrayExpression(ColumnExpr, AliasExpr(ColumnExpr || ConstantExpr || Expression), ...)
//schema may change
DataFrame& DataFrame::Select(std::shared_ptr<Expression> expr) {
  try {
  } catch (ExpressionException& e) {

  } catch (boost::bad_any_cast& e) {
  
  }
  return *this;
}
//PredicateExpression
//schema not change
DataFrame& DataFrame::Where(std::shared_ptr<Expression> expr) {
  
  try {
    expr->Resolve(schema_);
    std::vector<std::shared_ptr<Row>> new_rows;
    for (auto &row : rows_) {
      std::shared_ptr<DataField> predicate_result = expr->Eval(row);
      if (boost::any_cast<bool>(predicate_result->cell->value())) {
        new_rows.push_back(std::move(row));
      }
    }
    rows_ = std::move(new_rows);
  } catch (ExpressionException& e) {

  } catch (boost::bad_any_cast& e) {
  
  }
  return *this;
}
//ArrayExpression(ColumnExpr, ...)
//schema may change
DataFrame& DataFrame::GroupBy(std::shared_ptr<Expression> expr) {
  try {
  } catch (ExpressionException& e) {

  } catch (boost::bad_any_cast& e) {
  
  }
  return *this;
}
//ArrayExpression(PairExpression(ColumnExpr, ColumnExpr), ...)
//schema may change
DataFrame& DataFrame::Join(const DataFrame& df,
    JoinType join_type,
    std::shared_ptr<Expression> join_expr) {
  try {
  } catch (ExpressionException& e) {

  } catch (boost::bad_any_cast& e) {
  
  }
  return *this;
}
//ArrayExpression(ColumnExpr, ...)
//schema not change
DataFrame& DataFrame::OrderBy(std::shared_ptr<Expression> expr) {
  try {
  } catch (ExpressionException& e) {

  } catch (boost::bad_any_cast& e) {
  
  }
  return *this;
}
//schema not change
DataFrame& DataFrame::Union(const DataFrame& df) {
  try {
  } catch (ExpressionException& e) {

  } catch (boost::bad_any_cast& e) {
  
  }
  return *this;
}
//avg max min sum count
//ArrayExpression(PairExpression(ColumnExpr, AggExpression), ...)
//schema may change
DataFrame& DataFrame::Agg(std::shared_ptr<Expression> expr) {
  try {
  } catch (ExpressionException& e) {

  } catch (boost::bad_any_cast& e) {
  
  }
  return *this;
}
}
