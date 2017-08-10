#include "data_frame.h"
#include <boost/any.hpp>
#include <glog/logging.h>
#include "column.h"
#include "data_frame_exception.h"
#include "data_field.h"
#include "expression/expression.h"
#include "expression/alias_expression.h"
#include "row.h"
#include "schema.h"

namespace mortred {

using namespace expression;

//ArrayExpression(ColumnExpr, AliasExpr(ColumnExpr || ConstantExpr || Expression), ...)
//schema may change
DataFrame& DataFrame::Select(std::shared_ptr<Expression> expr) {
  try {
    expr->Resolve(schema_);
    if (expr->node_type() != NodeType::ARRAY) {
      LOG(INFO) << "not supported expression type";
      throw DataFrameException("not supported expression type");
    }
    std::vector<std::shared_ptr<Column>> new_columns;
    for (auto& child : expr->GetChildren()) {
      switch (child->node_type()) {
        case NodeType::COLUMN: {
          std::shared_ptr<ColumnExpr> column_expr = std::static_pointer_cast<ColumnExpr>(child);
          new_columns.push_back(std::make_shared<Column>(column_expr->column_name(), column_expr->data_type(), column_expr->nullable()));
          break;
        }
        case NodeType::ALIAS: {
          std::shared_ptr<AliasExpr> alias_expr = std::static_pointer_cast<AliasExpr>(child);
          new_columns.push_back(std::make_shared<Column>(alias_expr->alias_name(), alias_expr->data_type(), alias_expr->nullable()));
          break;
        }
        default: {
          LOG(INFO) << "not supported expression type";
          throw DataFrameException("not supported expression type");
        }
      }
    }
    std::shared_ptr<Schema> new_schema = std::make_shared<Schema>(new_columns);
    std::vector<std::shared_ptr<Row>> new_rows;
    for (auto& row : rows_) {
    }
  } catch (ExpressionException& e) {
    throw DataFrameException(e.what());
  } catch (boost::bad_any_cast& e) {
    throw DataFrameException(e.what());
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
    throw DataFrameException(e.what());
  } catch (boost::bad_any_cast& e) {
    throw DataFrameException(e.what());
  }
  return *this;
}
//ArrayExpression(ColumnExpr, ...)
//schema may change
DataFrame& DataFrame::GroupBy(std::shared_ptr<Expression> expr) {
  try {
  } catch (ExpressionException& e) {
    throw DataFrameException(e.what());
  } catch (boost::bad_any_cast& e) {
    throw DataFrameException(e.what());
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
    throw DataFrameException(e.what());
  } catch (boost::bad_any_cast& e) {
    throw DataFrameException(e.what());
  }
  return *this;
}
//ArrayExpression(ColumnExpr, ...)
//schema not change
DataFrame& DataFrame::OrderBy(std::shared_ptr<Expression> expr) {
  try {
  } catch (ExpressionException& e) {
    throw DataFrameException(e.what());
  } catch (boost::bad_any_cast& e) {
    throw DataFrameException(e.what());
  }
  return *this;
}
//schema not change
DataFrame& DataFrame::Union(const DataFrame& df) {
  try {
  } catch (ExpressionException& e) {
    throw DataFrameException(e.what());
  } catch (boost::bad_any_cast& e) {
    throw DataFrameException(e.what());
  }
  return *this;
}
//avg max min sum count
//ArrayExpression(PairExpression(ColumnExpr, AggExpression), ...)
//schema may change
DataFrame& DataFrame::Agg(std::shared_ptr<Expression> expr) {
  try {
  } catch (ExpressionException& e) {
    throw DataFrameException(e.what());
  } catch (boost::bad_any_cast& e) {
    throw DataFrameException(e.what());
  }
  return *this;
}
}
