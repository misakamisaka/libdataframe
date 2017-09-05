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
  //ArrayExpression(ColumnExpr, AliasExpr(ColumnExpr || ConstantExpr || Expression), ...)
  DataFrame& Select(std::shared_ptr<expression::Expression> expr);
  //PredicateExpression
  DataFrame& Where(std::shared_ptr<expression::Expression> expr);
  //ArrayExpression(ColumnExpr, ...)
  DataFrame& GroupBy(std::shared_ptr<expression::Expression> expr);
  //ArrayExpression(PairExpression(ColumnExpr, ColumnExpr), ...)
  DataFrame& Join(DataFrame& df,
      JoinType join_type,
      std::shared_ptr<expression::Expression> join_expr);
  //ArrayExpression(ColumnExpr, ...)
  DataFrame& OrderBy(std::shared_ptr<expression::Expression> expr);
  DataFrame& Union(const DataFrame& df);
  DataFrame& Dinstinct();
  //avg max min sum count
  //ArrayExpression(PairExpression(ColumnExpr, AggExpression), ...)
  DataFrame& Agg(std::shared_ptr<expression::Expression> expr);

  explicit DataFrame(std::shared_ptr<Schema> schema);
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
