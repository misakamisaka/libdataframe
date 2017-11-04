#include "data_frame.h"
#include <algorithm>
#include <functional>
#include <iomanip>
#include <set>
#include <sstream>

#include <boost/any.hpp>

#include <glog/logging.h>

#include "column.h"
#include "data_frame_exception.h"
#include "data_field.h"
#include "expression/expression.h"
#include "expression/alias_expression.h"
#include "expression/type_cast_expression.h"
#include "row.h"
#include "schema.h"
#include "type/type.h"
#include "util.h"


namespace mortred {

using namespace expression;

DataFrame::DataFrame(std::shared_ptr<Schema> schema, const std::vector<std::shared_ptr<Row>>& rows)
  :schema_(schema), rows_(rows) { }
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
        case NodeType::CONVERT: {
          std::shared_ptr<TypeCastExpr> type_cast_expr = std::static_pointer_cast<TypeCastExpr>(child);
          new_columns.push_back(std::make_shared<Column>(type_cast_expr->column_name(), type_cast_expr->data_type(), type_cast_expr->nullable()));
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
        cells.push_back(child->Eval(row)->cell());
      }
      new_rows.push_back(std::make_shared<Row>(cells));
    }
    schema_ = std::move(new_schema);
    rows_ = std::move(new_rows);
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
      if (boost::any_cast<bool>(predicate_result->cell()->value())) {
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
    std::set<int> column_index_set;
    std::vector<int> group_index_vec;
    expr->Resolve(schema_);
    SortByExpression(expr);
    for (auto& child : expr->GetChildren()) {
      column_index_set.insert(std::static_pointer_cast<ColumnExpr>(child)->index());
    }

    std::vector<std::shared_ptr<Column>> new_columns;
    std::vector<std::shared_ptr<Column>> group_columns;
    for (size_t i = 0; i < schema_->columns().size(); ++i) {
      if (column_index_set.find(i) != column_index_set.end()) {
        new_columns.push_back(schema_->GetColumnByIndex(i));
      } else {
        group_index_vec.push_back(i);
        group_columns.push_back(schema_->GetColumnByIndex(i));
      }
    }
    for (auto& group_column : group_columns) {
      new_columns.push_back(std::make_shared<Column>(group_column->name(), std::make_shared<ListType>(group_column)));
    }
    std::shared_ptr<Schema> new_schema = std::make_shared<Schema>(new_columns, group_columns.size());
    std::vector<std::shared_ptr<Row>> new_rows;
    static auto row_equal = 
      [&column_index_set, this](const std::shared_ptr<Row>& row1, const std::shared_ptr<Row>& row2){
        for (auto& col_idx : column_index_set) {
          std::shared_ptr<DataType> data_type = schema_->GetColumnByIndex(col_idx)->data_type();
          if (!DataField(row1->at(col_idx), data_type).Equal(DataField(row2->at(col_idx), data_type))) {
            return false;
          }
        }
        return true;
      };
    for (size_t current = 0, cursor = 1; cursor <= rows_.size(); ++cursor) {
      if (cursor == rows_.size() ||
            !row_equal(rows_[current], rows_[cursor])) {
        std::vector<std::shared_ptr<Cell>> cells;
        for (auto& col_idx : column_index_set) {
          cells.push_back(rows_[current]->at(col_idx));
        }
        for (auto& grp_idx : group_index_vec) {
          std::vector<std::shared_ptr<Cell>> temp_value;
          for (size_t i = current; i < cursor; ++i) {
            temp_value.push_back(rows_[i]->at(grp_idx));
          }
          cells.push_back(std::make_shared<Cell>(false, temp_value));
        }
        new_rows.push_back(std::make_shared<Row>(cells));
        current = cursor;
      }
    }
    
    schema_ = std::move(new_schema);
    rows_ = std::move(new_rows);

  } catch (MortredException& e) {
    throw DataFrameException(e.what());
  }
  return *this;
}
//PairExpression(ArrayExpression(ColumnExpr, ColumnExpr, ...), ArrayExpression(ColumnExpr, ColumnExpr, ...))
//schema may change
DataFrame& DataFrame::Join(DataFrame& df,
    JoinType join_type,
    std::shared_ptr<Expression> join_expr) {
  try {
    ASSERT_EXPR_TYPE(join_expr, NodeType::PAIR)
    
    std::shared_ptr<PairExpression> pair_expr = std::static_pointer_cast<PairExpression>(join_expr);
    std::shared_ptr<Expression> left_expr = pair_expr->left();
    std::shared_ptr<Expression> right_expr = pair_expr->right();
    ASSERT_EXPR_TYPE(left_expr, NodeType::ARRAY)
    ASSERT_EXPR_TYPE(right_expr, NodeType::ARRAY)
    if (left_expr->GetChildren().size() != right_expr->GetChildren().size()) {
      throw DataFrameException("join left column and right column size not match");
    }
    left_expr->Resolve(schema_);
    right_expr->Resolve(df.schema_);
      //SortByExpression(std::make_shared<ArrayExpression>(left_table_columns));
      //df.SortByExpression(std::make_shared<ArrayExpression>(right_table_columns));
      //klogk + nlogn
      //sort left
      //sort right
      // for each row in left & right table
      //    if(left_key == right_key)
      //      l = left 
      //      while (l_key == r_key)
      //        r = right
      //        while(l_key == r_key)
      //          join and write to out
      //          ++r
      //        ++l
      //      
      //    else if left_key < right_key
      //      if (left join) 
      //        join and write to out
      //      ++ left
      //    else if left_key > right_key
      //      if (right join)
      //        join and write to out
      //      ++ right

      //klogn + nlogn
      //sort right
      // inner and left join
      // for each row in left table
      //  get lower_bound of left_key in right table
      //  join and write

    switch (join_type) {
      case JoinType::INNER: {
        if (rows_.size() <= df.rows_.size()) {
          MyJoin(*this, df, left_expr, right_expr, false);
        } else {
          MyJoin(df, *this, right_expr, left_expr, false);
        }
        break;
      }
      case JoinType::LEFT:
          MyJoin(*this, df, left_expr, right_expr, false);
        break;
      case JoinType::RIGHT:
          MyJoin(df, *this, right_expr, left_expr, false);
        break;
      default:
        throw DataFrameException("not supported join type");
    }
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
    SortByExpression(expr);
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

DataFrame& DataFrame::Distinct() {
  Sort();
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
//avg max min sum count first last
//ArrayExpression(AliasExpression(AggExpression()), ...)
//schema may change
DataFrame& DataFrame::Agg(std::shared_ptr<Expression> expr) {
  try {
    ASSERT_EXPR_TYPE(expr, NodeType::ARRAY)
    expr->Resolve(schema_);
    std::vector<std::shared_ptr<Column>> new_columns(schema_->columns());
    for (auto& child : expr->GetChildren()) {
      ASSERT_EXPR_TYPE(child, NodeType::ALIAS)
      std::shared_ptr<AliasExpr> alias_expr = std::static_pointer_cast<AliasExpr>(child);
      new_columns.push_back(std::make_shared<Column>(alias_expr->alias_name(), alias_expr->data_type(), alias_expr->nullable()));
    }
    std::shared_ptr<Schema> new_schema = std::make_shared<Schema>(new_columns);
    std::vector<std::shared_ptr<Row>> new_rows;
    for (auto& row : rows_) {
      std::vector<std::shared_ptr<Cell>> cells(row->cells());
      for (auto& child : expr->GetChildren()) {
        cells.push_back(child->Eval(row)->cell());
      }
      new_rows.push_back(std::make_shared<Row>(cells));
    }
    schema_ = std::move(new_schema);
    rows_ = std::move(new_rows);
  } catch (MortredException& e) {
    throw DataFrameException(e.what());
  }
  return *this;
}

void DataFrame::Sort() {
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
}

void DataFrame::SortByExpression(const std::shared_ptr<Expression>& expr) {
  ASSERT_EXPR_TYPE(expr, NodeType::ARRAY)
  for (auto& child : expr->GetChildren()) {
    if (child->node_type() != NodeType::COLUMN) {
      LOG(INFO) << "child of order by expr must be ColumnExpr";
      throw DataFrameException("not supported expression type");
    }
  }
  stable_sort(rows_.begin(), rows_.end(), [&expr](const std::shared_ptr<Row>& row1, const std::shared_ptr<Row>& row2) {
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
        const std::shared_ptr<DataField>& data_field2) {
        return data_field1->LessThan(data_field2);
      });
  });
}

void DataFrame::MyJoin(DataFrame& left,
    DataFrame& right,
    const std::shared_ptr<Expression>& left_expr,
    const std::shared_ptr<Expression>& right_expr,
    bool is_outer) {
  right.SortByExpression(right_expr);
  std::set<size_t> right_duplicate_index;
  for (size_t i = 0 ; i < left_expr->GetChildren().size(); ++i) {
    std::shared_ptr<ColumnExpr> left_column_expr = std::static_pointer_cast<ColumnExpr>(left_expr->GetChildren()[i]);
    std::shared_ptr<ColumnExpr> right_column_expr = std::static_pointer_cast<ColumnExpr>(right_expr->GetChildren()[i]);
    if (left_column_expr->column_name() == right_column_expr->column_name()) {
      right_duplicate_index.insert(right_column_expr->index());
    }
  }
  std::vector<std::shared_ptr<Column>> new_columns(left.schema_->columns());
  for (size_t i = 0; i < right.schema_->columns().size(); ++i) {
    if (right_duplicate_index.find(i) == right_duplicate_index.end()) {
      new_columns.push_back(right.schema_->columns()[i]);
    }
  }
  std::shared_ptr<Schema> new_schema = std::make_shared<Schema>(new_columns);
  std::vector<std::shared_ptr<Row>> new_rows;
  for (auto& row : left.rows_) {
    auto it = lower_bound(right.rows_.begin(),
        right.rows_.end(),
        row,
        [&left_expr, &right_expr](const std::shared_ptr<Row>& row1,
          const std::shared_ptr<Row>& row2) {
          std::vector<std::shared_ptr<DataField>> data_fields1;
          std::vector<std::shared_ptr<DataField>> data_fields2;
          for (size_t i = 0; i < left_expr->GetChildren().size(); ++i) {
            data_fields1.push_back(right_expr->GetChildren()[i]->Eval(row1));
            data_fields2.push_back(left_expr->GetChildren()[i]->Eval(row2));
          }

          return lexicographical_compare(data_fields1.begin(),
            data_fields1.end(),
            data_fields2.begin(),
            data_fields2.end(),
            [](const std::shared_ptr<DataField>& data_field1,
              const std::shared_ptr<DataField>& data_field2) {
              return data_field1->LessThan(data_field2);
            });
        });
    static auto row_equal = 
      [&left_expr, &right_expr](const std::shared_ptr<Row>& row1, const std::shared_ptr<Row>& row2){
        
        for (size_t i = 0; i < left_expr->GetChildren().size(); ++i) {
          if (!right_expr->GetChildren()[i]->Eval(row2)->Equal(left_expr->GetChildren()[i]->Eval(row1))) {
            return false;
          }
        }
        return true;
      };
    bool flag = true;
    while (it != right.rows_.end() && row_equal(row, *it)) {
      std::vector<std::shared_ptr<Cell>> cells(row->cells());
      for (size_t i = 0; i < (*it)->cells().size(); ++i) {
        if (right_duplicate_index.find(i) == right_duplicate_index.end()) {
          cells.push_back((*it)->cells()[i]);
        }
      }
      new_rows.push_back(std::make_shared<Row>(cells));
      flag = false;
      ++it;
    }
    if (is_outer && flag) {
      std::vector<std::shared_ptr<Cell>> cells(row->cells());
      cells.insert(cells.end(), right.schema_->columns().size() - right_duplicate_index.size(), std::make_shared<Cell>());
      new_rows.push_back(std::make_shared<Row>(cells));
    }
  }
  schema_ = std::move(new_schema);
  rows_ = std::move(new_rows);
}
void DataFrame::Print() {
  std::ostringstream col_oss;
  for (const auto& column : schema_->columns()) {
    col_oss << std::setw(10) << column->name() +"(" + column->data_type()->ToString() + ")" << "|";
  }
  LOG(INFO) << col_oss.str();
  for (auto& row: rows_) {
    std::ostringstream row_oss;
    for (size_t i = 0; i < row->size(); ++i) {
      std::shared_ptr<DataType> data_type = schema_->GetColumnByIndex(i)->data_type();
      DataField datafield(row->at(i), data_type);
      row_oss << std::setw(10) <<  datafield.ToString() << "|";
    }
    LOG(INFO) << row_oss.str();
  }
}
}
