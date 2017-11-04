#ifndef MORTRED_DATA_FRAME_H
#define MORTRED_DATA_FRAME_H

#include <memory>
#include <vector>

namespace mortred {

class Schema;
class Row;

namespace expression {
class Expression;
}


enum class JoinType {
  INNER,
  LEFT,
  RIGHT,
};

class DataFrame {
 public:
  //ArrayExpression(ColumnExpr, AliasExpr(ColumnExpr || ConstantExpr || Expression), CastExpr(ColumnExpr || ...),...)
  DataFrame& Select(std::shared_ptr<expression::Expression> expr);
  //PredicateExpression
  DataFrame& Where(std::shared_ptr<expression::Expression> expr);
  //ArrayExpression(ColumnExpr, ...)
  DataFrame& GroupBy(std::shared_ptr<expression::Expression> expr);
  //PairExpression(ArrayExpression(ColumnExpr, ColumnExpr), ...)
  DataFrame& Join(DataFrame& df,
      JoinType join_type,
      std::shared_ptr<expression::Expression> join_expr);
  //ArrayExpression(ColumnExpr, ...)
  DataFrame& OrderBy(std::shared_ptr<expression::Expression> expr);
  DataFrame& Union(const DataFrame& df);
  DataFrame& Distinct();
  //avg max min sum count
  //ArrayExpression(PairExpression(ColumnExpr, AggExpression), ...)
  DataFrame& Agg(std::shared_ptr<expression::Expression> expr);

  DataFrame(std::shared_ptr<Schema> schema, const std::vector<std::shared_ptr<Row>>& rows);

  DataFrame(const DataFrame& dataframe)
    :schema_(dataframe.schema_), rows_(dataframe.rows_) {}
  
  void Print();
  std::vector<std::shared_ptr<Row>> rows() {
    return rows_;
  }
  std::shared_ptr<Schema> schema() {
    return schema_;
  }
 private:
  void Sort();
  void SortByExpression(const std::shared_ptr<expression::Expression>& expr);
  void MyJoin(DataFrame& left,
      DataFrame& right,
      const std::shared_ptr<expression::Expression>& left_expr,
      const std::shared_ptr<expression::Expression>& right_expr,
      bool is_outer);
 private:
  std::shared_ptr<Schema> schema_;
  std::vector<std::shared_ptr<Row>> rows_;
};

}

#endif
