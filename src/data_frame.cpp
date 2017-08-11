#include "data_frame.h"
#include <algorithm>
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
    if (expr->node_type() != NodeType::ARRAY) {
      LOG(INFO) << "not supported expression type";
      throw DataFrameException("not supported expression type");
    }
    expr->Resolve(schema_);
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
      std::vector<std::shared_ptr<Cell>> cells;
      for (auto& child : expr->GetChildren()) {
        cells.push_back(child->Eval(row)->cell);
      }
      new_rows.push_back(std::make_shared<Row>(cells));
    }
    schema_ = new_schema;
    rows_ = new_rows;
  } catch (MortredException& e) {
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
  } catch (MortredException& e) {
    throw DataFrameException(e.what());
  }
  return *this;
}
//ArrayExpression(ColumnExpr, ...)
//schema may change
DataFrame& DataFrame::GroupBy(std::shared_ptr<Expression> expr) {
  try {
  } catch (MortredException& e) {
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
  } catch (MortredException& e) {
    throw DataFrameException(e.what());
  }
  return *this;
}
//ArrayExpression(ColumnExpr, ...)
//schema not change
DataFrame& DataFrame::OrderBy(std::shared_ptr<Expression> expr) {
  try {
    expr->Resolve(schema_);
    if (expr->node_type() != NodeType::ARRAY) {
      LOG(INFO) << "order by only accept ArrayExpression";
      throw DataFrameException("not supported expression type");
    }
    for (auto& child : expr->GetChildren()) {
      if (child->node_type() != NodeType::COLUMN) {
        LOG(INFO) << "child of order by expr must be ColumnExpr";
        throw DataFrameException("not supported expression type");
      }
    }
    sort(rows_.begin(), rows_.end(), [&expr](const std::shared_ptr<Row>& row1, const std::shared_ptr<Row>& row2){
      std::vector<std::shared_ptr<DataField>> data_fields1;
      std::vector<std::shared_ptr<DataField>> data_fields2;
      for (auto& child : expr->GetChildren()) {
        data_fields1.push_back(child->Eval(row1));
        data_fields2.push_back(child->Eval(row2));
      }

      return lexicographical_compare(data_fields1.begin(),
        data_fields1.end(),
        data_fields2.begin(),
        data_fields2.end(),
        [](const std::shared_ptr<DataField>& data_field1,
          const std::shared_ptr<DataField>& data_field2){
          return data_field1->LessThan(data_field2);
        });
    });
  } catch (MortredException& e) {
    throw DataFrameException(e.what());
  }
  return *this;
}
//schema not change
DataFrame& DataFrame::Union(const DataFrame& df) {
  try {
  } catch (MortredException& e) {
    throw DataFrameException(e.what());
  }
  return *this;
}
//avg max min sum count
//ArrayExpression(PairExpression(ColumnExpr, AggExpression), ...)
//schema may change
DataFrame& DataFrame::Agg(std::shared_ptr<Expression> expr) {
  try {
  } catch (MortredException& e) {
    throw DataFrameException(e.what());
  }
  return *this;
}
}
