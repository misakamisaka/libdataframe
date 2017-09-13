#define GTEST_USE_OWN_TR1_TUPLE 0
#include <gtest/gtest.h>
#include "type/type.h"
#include "expression/expression.h"
#include "expression/alias_expression.h"
#include "expression/arithmetic_expression.h"
#include "expression/predicate_expression.h"
#include "expression/aggregate_expression.h"
#include "boost/any.hpp"
#include "row.h"
#include "schema.h"
#include "data_field.h"
#include "data_frame.h"
#include <memory>

using namespace mortred;
using namespace mortred::expression;

class ExpressionTest : public testing::Test {
public:
    ExpressionTest() {
        std::vector<std::shared_ptr<Column>> columns;
        columns.push_back(std::make_shared<Column>("A", DataTypes::MakeInt64Type()));
        columns.push_back(std::make_shared<Column>("B", DataTypes::MakeDoubleType()));
        columns.push_back(std::make_shared<Column>("C", DataTypes::MakeStringType()));
        columns.push_back(std::make_shared<Column>("D", DataTypes::MakeInt64Type()));
        columns.push_back(std::make_shared<Column>("E", DataTypes::MakeDoubleType()));
        columns.push_back(std::make_shared<Column>("F", DataTypes::MakeStringType()));
        columns.push_back(std::make_shared<Column>("G", DataTypes::MakeInt64Type()));
        columns.push_back(std::make_shared<Column>("H", DataTypes::MakeDoubleType()));
        columns.push_back(std::make_shared<Column>("I", DataTypes::MakeStringType()));
        columns.push_back(std::make_shared<Column>("J", DataTypes::MakeInt32Type()));
        columns.push_back(std::make_shared<Column>("K", DataTypes::MakeFloatType()));
        columns.push_back(std::make_shared<Column>("L", DataTypes::MakeUInt64Type()));
        columns.push_back(std::make_shared<Column>("M", DataTypes::MakeFloatType()));
        columns.push_back(std::make_shared<Column>("N", DataTypes::MakeBooleanType()));
        columns.push_back(std::make_shared<Column>("O", DataTypes::MakeInt32Type()));
    std::shared_ptr<Schema> schema = std::make_shared<Schema>(columns);
    std::vector<std::shared_ptr<Row>> rows;
    rows.push_back(std::make_shared<Row>(std::vector<std::shared_ptr<Cell>>({
            std::make_shared<Cell>(false, (int64_t)56),
            std::make_shared<Cell>(false, (double)25.1123),
            std::make_shared<Cell>(false, std::string("aaaaa")),
            std::make_shared<Cell>(false, (int64_t)1234),
            std::make_shared<Cell>(false, (double)123.5567),
            std::make_shared<Cell>(false, std::string("dafasdf")),
            std::make_shared<Cell>(false, (int64_t)100000000000000),
            std::make_shared<Cell>(false, (double)547567.978978),
            std::make_shared<Cell>(false, std::string("asdadasd")),
            std::make_shared<Cell>(false, (int32_t)10456797),
            std::make_shared<Cell>(false, (float)1023.209),
            std::make_shared<Cell>(false, (uint64_t)10034895763495),
            std::make_shared<Cell>(false, (float)100),
            std::make_shared<Cell>(false, false),
            std::make_shared<Cell>(false, (int32_t)435345)
        })));
    rows.push_back(std::make_shared<Row>(std::vector<std::shared_ptr<Cell>>({
            std::make_shared<Cell>(false, (int64_t)56),
            std::make_shared<Cell>(false, (double)25.1123),
            std::make_shared<Cell>(false, std::string("aaaaa")),
            std::make_shared<Cell>(false, (int64_t)1234),
            std::make_shared<Cell>(false, (double)123.5567),
            std::make_shared<Cell>(false, std::string("dafasdf")),
            std::make_shared<Cell>(false, (int64_t)100000000000000),
            std::make_shared<Cell>(false, (double)547567.978978),
            std::make_shared<Cell>(false, std::string("asdadasd")),
            std::make_shared<Cell>(false, (int32_t)10456797),
            std::make_shared<Cell>(false, (float)1023.209),
            std::make_shared<Cell>(false, (uint64_t)10034895763495),
            std::make_shared<Cell>(false, (float)100),
            std::make_shared<Cell>(false, false),
            std::make_shared<Cell>(false, (int32_t)435345)
        })));
    rows.push_back(std::make_shared<Row>(std::vector<std::shared_ptr<Cell>>({
            std::make_shared<Cell>(false, (int64_t)56),
            std::make_shared<Cell>(false, (double)25.1123),
            std::make_shared<Cell>(false, std::string("aaaaa")),
            std::make_shared<Cell>(false, (int64_t)1234),
            std::make_shared<Cell>(false, (double)123.55678),
            std::make_shared<Cell>(false, std::string("dafasdfaa")),
            std::make_shared<Cell>(false, (int64_t)1000000000000000),
            std::make_shared<Cell>(false, (double)547567.9789),
            std::make_shared<Cell>(false, std::string("asdadaasdasdsd")),
            std::make_shared<Cell>(false, (int32_t)104567),
            std::make_shared<Cell>(false, (float)10.209),
            std::make_shared<Cell>(false, (uint64_t)103495),
            std::make_shared<Cell>(false, (float)10.5),
            std::make_shared<Cell>(false, true),
            std::make_shared<Cell>(false, (int32_t)435)
        })));
    rows.push_back(std::make_shared<Row>(std::vector<std::shared_ptr<Cell>>({
            std::make_shared<Cell>(false, (int64_t)1),
            std::make_shared<Cell>(false, (double)-2.2),
            std::make_shared<Cell>(false, std::string("bbbbb")),
            std::make_shared<Cell>(false, (int64_t)-123),
            std::make_shared<Cell>(false, (double)167),
            std::make_shared<Cell>(false, std::string("dasdf")),
            std::make_shared<Cell>(false, (int64_t)1000),
            std::make_shared<Cell>(false, (double)54.78),
            std::make_shared<Cell>(false, std::string("asd")),
            std::make_shared<Cell>(false, (int32_t)104),
            std::make_shared<Cell>(false, (float)-123.123),
            std::make_shared<Cell>(false, (uint64_t)195),
            std::make_shared<Cell>(false, (float)100),
            std::make_shared<Cell>(false, false),
            std::make_shared<Cell>(false, (int32_t)435)
        })));
    rows.push_back(std::make_shared<Row>(std::vector<std::shared_ptr<Cell>>({
            std::make_shared<Cell>(false, (int64_t)1),
            std::make_shared<Cell>(false, (double)2.2),
            std::make_shared<Cell>(false, std::string("bbbbb")),
            std::make_shared<Cell>(false, (int64_t)123),
            std::make_shared<Cell>(false, (double)167),
            std::make_shared<Cell>(false, std::string("dasdf")),
            std::make_shared<Cell>(false, (int64_t)1000),
            std::make_shared<Cell>(false, (double)54.78),
            std::make_shared<Cell>(false, std::string("asd")),
            std::make_shared<Cell>(false, (int32_t)104),
            std::make_shared<Cell>(false, (float)123.123),
            std::make_shared<Cell>(false, (uint64_t)195),
            std::make_shared<Cell>(false, (float)100),
            std::make_shared<Cell>(false, false),
            std::make_shared<Cell>(false, (int32_t)435)
        })));
      dataframe = std::make_shared<DataFrame>(schema, rows);
    }
protected:
    virtual void SetUp() {
    }
public:
    std::shared_ptr<DataFrame> dataframe;
};

TEST_F(ExpressionTest, test_add) {
  DataFrame dataframe2(*dataframe);
  DataFrame dataframe3(*dataframe);
  dataframe->Print();
    //test int param add param
    {
        std::shared_ptr<ColumnExpr> param1 = std::make_shared<ColumnExpr>("A");
        std::shared_ptr<ColumnExpr> param2 = std::make_shared<ColumnExpr>("D");
        std::vector<std::shared_ptr<Expression>> exprs;
        exprs.push_back(param1);
        exprs.push_back(param2);
        std::shared_ptr<ArrayExpression> expression = std::make_shared<ArrayExpression>(exprs);
        (*dataframe).Select(expression).Print();
        (*dataframe).Distinct().Print();
    }
  dataframe2.Print();
  dataframe2.OrderBy(std::make_shared<ArrayExpression>(std::vector<std::shared_ptr<Expression>>({std::make_shared<ColumnExpr>("G"), std::make_shared<ColumnExpr>("D"),std::make_shared<ColumnExpr>("M")}))).Print();
    //test int constant add constant
    {
        std::shared_ptr<ColumnExpr> param1 = std::make_shared<ColumnExpr>("D");
        std::shared_ptr<ConstantExpr> param2 = std::make_shared<ConstantExpr>(false, "178", DataTypes::MakeInt64Type());
        std::shared_ptr<GreaterThan> expression = std::make_shared<GreaterThan>(param1, param2);
        dataframe2.Where(expression).Print();
    }
  dataframe->Union(dataframe2.Select(std::make_shared<ArrayExpression>(std::vector<std::shared_ptr<Expression>>({std::make_shared<ColumnExpr>("A"), std::make_shared<ColumnExpr>("D")}))).Distinct());
  dataframe->Print();
  dataframe3.Print();
  dataframe->Join(dataframe3, JoinType::INNER, std::make_shared<PairExpression>(std::make_shared<ArrayExpression>(std::vector<std::shared_ptr<Expression>>({std::make_shared<ColumnExpr>("A"), std::make_shared<ColumnExpr>("D")})), std::make_shared<ArrayExpression>(std::vector<std::shared_ptr<Expression>>({std::make_shared<ColumnExpr>("A"), std::make_shared<ColumnExpr>("D")}))));
  dataframe->Print();
  dataframe->Select(std::make_shared<ArrayExpression>(std::vector<std::shared_ptr<Expression>>({std::make_shared<ColumnExpr>("A"),std::make_shared<ColumnExpr>("D"),std::make_shared<AliasExpr>(std::make_shared<AddExpr>(std::make_shared<ColumnExpr>("G"),std::make_shared<ConstantExpr>(false, "123.1", DataTypes::MakeFloatType())), "asdasd")})));
  dataframe->Print();
  dataframe->GroupBy(std::make_shared<ArrayExpression>(std::vector<std::shared_ptr<Expression>>({std::make_shared<ColumnExpr>("A")})));
  dataframe->Print();
  dataframe->Agg(std::make_shared<ArrayExpression>(std::vector<std::shared_ptr<Expression>>({std::make_shared<AliasExpr>(std::make_shared<AvgExpr>("D"), "GG"), std::make_shared<AliasExpr>(std::make_shared<MaxExpr>("asdasd"), "asdasdaaa"), std::make_shared<AliasExpr>(std::make_shared<SumExpr>("asdasd"), "asdasdaaagg"), std::make_shared<AliasExpr>(std::make_shared<CountExpr>("asdasd"), "axxsdasdaaa"), std::make_shared<AliasExpr>(std::make_shared<FirstExpr>("asdasd"), "gfsdgasdasdaaa")})));

  dataframe->Print();
/*
    //test int constant add constant
    {
        std::shared_ptr<ConstantExpr> param1 = std::make_shared<ConstantExpr>(false, "123", DataTypes::MakeInt64Type());
        std::shared_ptr<ConstantExpr> param2 = std::make_shared<ConstantExpr>(false, "78", DataTypes::MakeInt64Type());
        std::shared_ptr<AddExpr> expression = std::make_shared<AddExpr>(param1, param2);
        expression->Resolve(schema);
        std::shared_ptr<DataField> result = expression->Eval(row);
        ASSERT_EQ(boost::any_cast<int64_t>(result->cell->value()), 78 + 123);
    }

    //test int constant add double constant
    {
        std::shared_ptr<ConstantExpr> param1 = std::make_shared<ConstantExpr>(false, "123", DataTypes::MakeInt64Type());
        std::shared_ptr<ConstantExpr> param2 = std::make_shared<ConstantExpr>(false, "78.3", DataTypes::MakeDoubleType());
        std::shared_ptr<AddExpr> expression = std::make_shared<AddExpr>(param1, param2);
        expression->Resolve(schema);
        std::shared_ptr<DataField> result = expression->Eval(row);
        ASSERT_EQ(boost::any_cast<double>(result->cell->value()), 78.3 + 123);
    }
    //test double param add int param
    {
        std::shared_ptr<ColumnExpr> param1 = std::make_shared<ColumnExpr>("A");
        std::shared_ptr<ColumnExpr> param2 = std::make_shared<ColumnExpr>("B");
        std::shared_ptr<AddExpr> expression = std::make_shared<AddExpr>(param1, param2);
        expression->Resolve(schema);
        std::shared_ptr<DataField> result = expression->Eval(row);
        ASSERT_EQ(boost::any_cast<double>(result->cell->value()), 25.1123 + 56);
    }

    {
        std::shared_ptr<ColumnExpr> param1 = std::make_shared<ColumnExpr>("A");
        std::shared_ptr<ColumnExpr> param2 = std::make_shared<ColumnExpr>("B");
        std::shared_ptr<GreaterThan> expression = std::make_shared<GreaterThan>(param1, param2);
        expression->Resolve(schema);
        std::shared_ptr<DataField> result = expression->Eval(row);
        ASSERT_TRUE(boost::any_cast<bool>(result->cell->value()));
    }
    {
        std::shared_ptr<ColumnExpr> param1 = std::make_shared<ColumnExpr>("A");
        std::shared_ptr<ColumnExpr> param2 = std::make_shared<ColumnExpr>("D");
        //LessThanOrEqual expression(param1, param2);
        BinaryPredicate<std::less_equal, NodeType::LE, ComparisonPredicateResolvePolicy> expression(param1, param2);
        expression.Resolve(schema);
        std::shared_ptr<DataField> result = expression.Eval(row);
        ASSERT_TRUE(boost::any_cast<bool>(result->cell->value()));
    }

*/
}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
