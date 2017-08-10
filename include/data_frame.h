#ifndef MORTRED_DATA_FRAME_H
#define MORTRED_DATA_FRAME_H

#include <memory>
#include <vector>

namespace mortred {

class Schema;
class Cell;

namespace expression {
class Expression;
}

using Row = std::vector<std::shared_ptr<Cell>>;

enum class JoinType {
  INNER,
  OUTER,
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
    //ArrayExpression(PairExpression(AliasExpr(ColumnExpr), AliasExpr(ColumnExpr)), ...)
    DataFrame& Join(const DataFrame& df,
        JoinType join_type,
        std::shared_ptr<expression::Expression> join_expr);
    //ArrayExpression(ColumnExpr, ...)
    DataFrame& OrderBy(std::shared_ptr<expression::Expression> expr);
    DataFrame& Union(const DataFrame& df);
    //avg max min sum count
    //ArrayExpression(PairExpression(ColumnExpr, AggExpression), ...)
    DataFrame& Agg(std::shared_ptr<expression::Expression> expr);

    explicit DataFrame(std::shared_ptr<Schema> schema);
private:
    std::shared_ptr<Schema> schema_;
    std::vector<std::shared_ptr<Row>> rows_;
};

}

#endif
