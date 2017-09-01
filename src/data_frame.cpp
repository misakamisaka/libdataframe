#include "data_frame.h"
#include <algorithm>
#include <functional>
#include <boost/any.hpp>
#include <glog/logging.h>
#include "column.h"
#include "data_frame_exception.h"
#include "data_field.h"
#include "expression/expression.h"
#include "expression/alias_expression.h"
#include "row.h"
#include "schema.h"
#include "util.h"


namespace mortred {

using namespace expression;

//ArrayExpression(ColumnExpr, AliasExpr(ColumnExpr || ConstantExpr || Expression), ...)
//schema may change
DataFrame& DataFrame::Select(std::shared_ptr<Expression> expr) {
  try {
    ASSERT_EXPR_TYPE(expr, NodeType::ARRAY)
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
    ASSERT_EXPR_TYPE(join_expr, NodeType::ARRAY)

    std::vector<std::shared_ptr<Expression>> left_table_columns;
    std::vector<std::shared_ptr<Expression>> right_table_columns;
    
    for (auto& child : join_expr->GetChildren()) {
      ASSERT_EXPR_TYPE(child, NodeType::PAIR)
       
      std::shared_ptr<PairExpression> pair_expr = std::static_pointer_cast<PairExpression>(child);
      left_table_columns.push_back(pair_expr->left());
      right_table_columns.push_back(pair_expr->right());
    }
  } catch (MortredException& e) {
    throw DataFrameException(e.what());
  }
  return *this;
}
//ArrayExpression(ColumnExpr, ...)
//schema not change
DataFrame& DataFrame::OrderBy(std::shared_ptr<Expression> expr, bool stable) {
  try {
    expr->Resolve(schema_);
    ASSERT_EXPR_TYPE(expr, NodeType::ARRAY)
    for (auto& child : expr->GetChildren()) {
      if (child->node_type() != NodeType::COLUMN) {
        LOG(INFO) << "child of order by expr must be ColumnExpr";
        throw DataFrameException("not supported expression type");
      }
    }
    std::function<void (std::vector<std::shared_ptr<Row>>::iterator, std::vector<std::shared_ptr<Row>>::iterator, std::function<bool(const std::shared_ptr<Row>&, const std::shared_ptr<Row>&)>)> sort_func;
    if (stable) {
      sort_func = std::stable_sort<std::vector<std::shared_ptr<Row>>::iterator, std::function<bool(const std::shared_ptr<Row>&, const std::shared_ptr<Row>&)>>;
    } else {
      sort_func = std::sort<std::vector<std::shared_ptr<Row>>::iterator, std::function<bool(const std::shared_ptr<Row>&, const std::shared_ptr<Row>&)>>;
    }
    sort_func(rows_.begin(), rows_.end(), [&expr](const std::shared_ptr<Row>& row1, const std::shared_ptr<Row>& row2){
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
    if (!schema_->Equals(df.schema_)) {
      LOG(INFO) << "the schema of two dataframes are not same";
      throw DataFrameException("the schema of two dataframes are not same");
    }
    rows_.insert(rows_.end(), df.rows_.begin(), df.rows_.end());
  } catch (MortredException& e) {
    throw DataFrameException(e.what());
  }
  return *this;
}
DataFrame& DataFrame::Dinstinct() {
    std::stable_sort(rows_.begin(), rows_.end(), [this](const std::shared_ptr<Row>& row1, const std::shared_ptr<Row>& row2){
        size_t index = 0;
        size_t size1 = row1->size();
        size_t size2 = row2->size();

        while (index!=size1) {
          if (index == size2) {
            return false;
          } else {
            std::shared_ptr<DataType> data_type = schema_->GetColumnByIndex(index)->data_type();
            DataField datafield1(row1->at(index), data_type);
            DataField datafield2(row2->at(index), data_type);
            if (datafield2.LessThan(datafield1)) {
              return false;
            } else if (datafield1.LessThan(datafield2)) {
              return true;
            }
          }
          ++index;
        }
        return (index!=size2);
    });
  std::vector<std::shared_ptr<Row>> new_rows;
  auto first = rows_.begin();
  auto last = rows_.end();
  auto current = first;
  if (first == last)
    return *this;
  new_rows.push_back(*current);
  ++current;
  for (;current != last; ++current) {
    for (size_t i = 0; i < (*first)->size(); ++i){
      std::shared_ptr<DataType> data_type = schema_->GetColumnByIndex(i)->data_type();
      if (!DataField((*first)->at(i), data_type).Equal(DataField((*current)->at(i), data_type))) {
        first = current;
        new_rows.push_back(*current);
        break;
      }
    }
  }
  rows_ = std::move(new_rows);
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
