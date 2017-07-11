#ifndef MORTRED_DATA_FRAME_H
#define MORTRED_DATA_FRAME_H

namespace mortred {

class Schema;

using Row = std::vector<std::shared_ptr<Cell>>;

//need lazy?
class DataFrame {
public:
    //ArrayExpression(ColumnExpression, AliasExpression(ColumnExpression || ConstantExpression || Expression), ...)
    std::shared_ptr<DataFrame> Select(std::shared_ptr<Expression> expr);
    //PredicateExpression
    std::shared_ptr<DataFrame> Where(std::shared_ptr<Expression> expr);
    //ArrayExpression(ColumnExpression, ...)
    std::shared_ptr<DataFrame> GroupBy(std::shared_ptr<Expression> expr);
    //ArrayExpression(BinaryExpression(AliasExpression(ColumnExpression), AliasExpression(ColumnExpression)), ...)
    std::shared_ptr<DataFrame> Join(std::shared_ptr<DataFrame> df,
        JoinType join_type,
        std::shared_ptr<Expression> join_expr);
    //ArrayExpression(ColumnExpression, ...)
    std::shared_ptr<DataFrame> OrderBy(std::shared_ptr<Expression> expr);
    std::shared_ptr<DataFrame> Union(std::shared_ptr<DataFrame> df);
    //avg max min sum count
    //ArrayExpression(BinaryExpression(ColumnExpression, AggExpression), ...)
    std::shared_ptr<DataFrame> Agg(std::shared_ptr<Expression> expr);
private:
    std::shared_ptr<Schema> schema_;
    std::vector<std::shared_ptr<Row>>> rows_;
};

}

#endif
