#define GTEST_USE_OWN_TR1_TUPLE 0
#include <gtest/gtest.h>
#include "data_field.h"
#include "expression.h"
#include "boost/any.hpp"

using namespace aegir;

class ExpressionTest : public testing::Test {
public:
    ExpressionTest() {
    // A    B    C    D    E    F    G    H    I    J    K    L    M    N    O    P    Q
    // I    D    S    I    D    S    I    D    S    I32  F    U64  F    B    I32 ST   L(ST)
        std::shared_ptr<Table> table = std::make_shared<Table>();
        table->schema = std::make_shared<Schema>();
        table->schema->fields.push_back(std::make_shared<Field>("A", DataType(FieldType::INT64)));
        table->schema->fields.push_back(std::make_shared<Field>("B", DataType(FieldType::DOUBLE)));
        table->schema->fields.push_back(std::make_shared<Field>("C", DataType(FieldType::STRING)));
        table->schema->fields.push_back(std::make_shared<Field>("D", DataType(FieldType::INT64)));
        table->schema->fields.push_back(std::make_shared<Field>("E", DataType(FieldType::DOUBLE)));
        table->schema->fields.push_back(std::make_shared<Field>("F", DataType(FieldType::STRING)));
        table->schema->fields.push_back(std::make_shared<Field>("G", DataType(FieldType::INT64)));
        table->schema->fields.push_back(std::make_shared<Field>("H", DataType(FieldType::DOUBLE)));
        table->schema->fields.push_back(std::make_shared<Field>("I", DataType(FieldType::STRING)));
        table->schema->fields.push_back(std::make_shared<Field>("J", DataType(FieldType::INT32)));
        table->schema->fields.push_back(std::make_shared<Field>("K", DataType(FieldType::FLOAT)));
        table->schema->fields.push_back(std::make_shared<Field>("L", DataType(FieldType::UINT64)));
        table->schema->fields.push_back(std::make_shared<Field>("M", DataType(FieldType::FLOAT)));
        table->schema->fields.push_back(std::make_shared<Field>("N", DataType(FieldType::BOOL)));
        table->schema->fields.push_back(std::make_shared<Field>("O", DataType(FieldType::INT32)));
        table->schema->fields.push_back(std::make_shared<Field>("P", DataType(FieldType::STRUCT)));
        table->schema->fields.push_back(std::make_shared<Field>("Q", DataType(FieldType::LIST)));
        table->schema->name_to_index.insert(std::make_pair("A", 0));
        table->schema->name_to_index.insert(std::make_pair("B", 1));
        table->schema->name_to_index.insert(std::make_pair("C", 2));
        table->schema->name_to_index.insert(std::make_pair("D", 3));
        table->schema->name_to_index.insert(std::make_pair("E", 4));
        table->schema->name_to_index.insert(std::make_pair("F", 5));
        table->schema->name_to_index.insert(std::make_pair("G", 6));
        table->schema->name_to_index.insert(std::make_pair("H", 7));
        table->schema->name_to_index.insert(std::make_pair("I", 8));
        table->schema->name_to_index.insert(std::make_pair("J", 9));
        table->schema->name_to_index.insert(std::make_pair("K", 10));
        table->schema->name_to_index.insert(std::make_pair("L", 11));
        table->schema->name_to_index.insert(std::make_pair("M", 12));
        table->schema->name_to_index.insert(std::make_pair("N", 13));
        table->schema->name_to_index.insert(std::make_pair("O", 14));
        table->schema->name_to_index.insert(std::make_pair("P", 15));
        table->schema->name_to_index.insert(std::make_pair("Q", 16));
        table->schema->primary_key_index.push_back(1);
        {
            std::shared_ptr<Row> row = std::make_shared<Row>();
            row->op_type = OpType::UPDATE;
            {
                std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
                data_field->field = table->schema->fields[0];
                data_field->is_null = false;
                data_field->data = (int64_t)56;
                row->data_fields.push_back(data_field);
            }
            {
                std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
                data_field->field = table->schema->fields[1];
                data_field->is_null = false;
                data_field->data = (double)25.1123;
                row->data_fields.push_back(data_field);
            }
            {
                std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
                data_field->field = table->schema->fields[2];
                data_field->is_null = false;
                data_field->data = std::string("aaaaa");
                row->data_fields.push_back(data_field);
            }
            {
                std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
                data_field->field = table->schema->fields[3];
                data_field->is_null = false;
                data_field->data = (int64_t)1234;
                row->data_fields.push_back(data_field);
            }
            {
                std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
                data_field->field = table->schema->fields[4];
                data_field->is_null = false;
                data_field->data = (double)123.5567;
                row->data_fields.push_back(data_field);
            }
            {
                std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
                data_field->field = table->schema->fields[5];
                data_field->is_null = false;
                data_field->data = std::string("dafasdf");
                row->data_fields.push_back(data_field);
            }
            {
                std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
                data_field->field = table->schema->fields[6];
                data_field->is_null = false;
                data_field->data = (int64_t)100000000000000;
                row->data_fields.push_back(data_field);
            }
            {
                std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
                data_field->field = table->schema->fields[7];
                data_field->is_null = false;
                data_field->data = (double)547567.978978;
                row->data_fields.push_back(data_field);
            }
            {
                std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
                data_field->field = table->schema->fields[8];
                data_field->is_null = false;
                data_field->data = std::string("asdadasd");
                row->data_fields.push_back(data_field);
            }
            {
                std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
                data_field->field = table->schema->fields[9];
                data_field->is_null = false;
                data_field->data = (int32_t)10456797;
                row->data_fields.push_back(data_field);
            }
            {
                std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
                data_field->field = table->schema->fields[10];
                data_field->is_null = false;
                data_field->data = (float)1023.209;
                row->data_fields.push_back(data_field);
            }
            {
                std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
                data_field->field = table->schema->fields[11];
                data_field->is_null = false;
                data_field->data = (uint64_t)10034895763495;
                row->data_fields.push_back(data_field);
            }
            {
                std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
                data_field->field = table->schema->fields[12];
                data_field->is_null = false;
                data_field->data = (float)100;
                row->data_fields.push_back(data_field);
            }
            {
                std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
                data_field->field = table->schema->fields[13];
                data_field->is_null = false;
                data_field->data = false;
                row->data_fields.push_back(data_field);
            }
            {
                std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
                data_field->field = table->schema->fields[14];
                data_field->is_null = false;
                data_field->data = (int32_t)435345;
                row->data_fields.push_back(data_field);
            }
            {
                std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
                data_field->field = table->schema->fields[15];
                data_field->is_null = false;
                data_field->data = std::vector<boost::any>({boost::any((int64_t)134235), boost::any((double)3245.5), boost::any(std::string("adfasdf"))});
                row->data_fields.push_back(data_field);
            }
            {
                std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
                data_field->field = table->schema->fields[16];
                data_field->is_null = false;
                data_field->data = std::vector<boost::any>({boost::any(std::vector<boost::any>({boost::any((int64_t)134235), boost::any((double)3245.5), boost::any(std::string("adfasdf"))}))});
                row->data_fields.push_back(data_field);
            }
            table->rows.push_back(row);
        }
        table_ = table;
    }
protected:
    virtual void SetUp() {
    }
public:
    std::shared_ptr<Table> table_;
};

TEST_F(ExpressionTest, test_add) {
    //test int param add constant
    {
        std::shared_ptr<ParameterExpression> param1 = std::make_shared<ParameterExpression>();
        param1->node_type = NodeType::PARAMETER;
        param1->name = "A";
        std::shared_ptr<ConstantExpression> param2 = std::make_shared<ConstantExpression>();
        param2->node_type = NodeType::CONSTANT;
        param2->data_type = DataType(FieldType::INT64);
        param2->value = (int64_t)123;
        std::shared_ptr<BinaryExpression> expression = Expression::MakeBinary(
                NodeType::ADD,
                param1,
                param2
                );
        std::shared_ptr<DataField> result = expression->Calculate(table_->schema, table_->rows[0]);
        ASSERT_EQ(boost::any_cast<int64_t>(result->data), 123 + 56);
    }
    //test int constant add constant
    {
        std::shared_ptr<ConstantExpression> param1 = std::make_shared<ConstantExpression>();
        param1->node_type = NodeType::CONSTANT;
        param1->data_type = DataType(FieldType::INT64);
        param1->value = (int64_t)12300000000000;
        std::shared_ptr<ConstantExpression> param2 = std::make_shared<ConstantExpression>();
        param2->node_type = NodeType::CONSTANT;
        param2->data_type = DataType(FieldType::INT64);
        param2->value = (int64_t)123;
        std::shared_ptr<BinaryExpression> expression = Expression::MakeBinary(
                NodeType::ADD,
                param1,
                param2
                );
        std::shared_ptr<DataField> result = expression->Calculate(table_->schema, table_->rows[0]);
        ASSERT_EQ(boost::any_cast<int64_t>(result->data), 123 + 12300000000000);
    }
    //test int param add param
    {
        std::shared_ptr<ParameterExpression> param1 = std::make_shared<ParameterExpression>();
        param1->node_type = NodeType::PARAMETER;
        param1->name = "A";
        std::shared_ptr<ParameterExpression> param2 = std::make_shared<ParameterExpression>();
        param2->node_type = NodeType::PARAMETER;
        param2->name = "D";
        std::shared_ptr<BinaryExpression> expression = Expression::MakeBinary(
                NodeType::ADD,
                param1,
                param2
                );
        std::shared_ptr<DataField> result = expression->Calculate(table_->schema, table_->rows[0]);
        ASSERT_EQ(boost::any_cast<int64_t>(result->data), 1234 + 56);
    }
    //test double param add constant
    {
        std::shared_ptr<ParameterExpression> param1 = std::make_shared<ParameterExpression>();
        param1->node_type = NodeType::PARAMETER;
        param1->name = "B";
        std::shared_ptr<ConstantExpression> param2 = std::make_shared<ConstantExpression>();
        param2->node_type = NodeType::CONSTANT;
        param2->data_type = DataType(FieldType::DOUBLE);
        param2->value = (double)123.123;
        std::shared_ptr<BinaryExpression> expression = Expression::MakeBinary(
                NodeType::ADD,
                param1,
                param2
                );
        std::shared_ptr<DataField> result = expression->Calculate(table_->schema, table_->rows[0]);
        ASSERT_EQ(boost::any_cast<double>(result->data), 123.123 + 25.1123);
    }
    //test double constant add constant
    {
        std::shared_ptr<ConstantExpression> param1 = std::make_shared<ConstantExpression>();
        param1->node_type = NodeType::CONSTANT;
        param1->data_type = DataType(FieldType::DOUBLE);
        param1->value = (double)12300000000000.34534534;
        std::shared_ptr<ConstantExpression> param2 = std::make_shared<ConstantExpression>();
        param2->node_type = NodeType::CONSTANT;
        param2->data_type = DataType(FieldType::DOUBLE);
        param2->value = (double)123.11;
        std::shared_ptr<BinaryExpression> expression = Expression::MakeBinary(
                NodeType::ADD,
                param1,
                param2
                );
        std::shared_ptr<DataField> result = expression->Calculate(table_->schema, table_->rows[0]);
        ASSERT_EQ(boost::any_cast<double>(result->data), 123.11 + 12300000000000.34534534);
    }
    //test double param add param
    {
        std::shared_ptr<ParameterExpression> param1 = std::make_shared<ParameterExpression>();
        param1->node_type = NodeType::PARAMETER;
        param1->name = "B";
        std::shared_ptr<ParameterExpression> param2 = std::make_shared<ParameterExpression>();
        param2->node_type = NodeType::PARAMETER;
        param2->name = "E";
        std::shared_ptr<BinaryExpression> expression = Expression::MakeBinary(
                NodeType::ADD,
                param1,
                param2
                );
        std::shared_ptr<DataField> result = expression->Calculate(table_->schema, table_->rows[0]);
        ASSERT_EQ(boost::any_cast<double>(result->data), 25.1123 + 123.5567);
    }
}

TEST_F(ExpressionTest, test_event_type) {
    std::shared_ptr<EventTypeExpression> expr = std::make_shared<EventTypeExpression>();
    expr->node_type = NodeType::EVENT_TYPE;
    std::shared_ptr<DataField> df = expr->Calculate(table_->schema, table_->rows[0]);
    ASSERT_EQ(boost::any_cast<int64_t>(df->data), 1);
}

TEST_F(ExpressionTest, test_convert) {
    {
        std::shared_ptr<ConstantExpression> param1 = std::make_shared<ConstantExpression>();
        param1->node_type = NodeType::CONSTANT;
        param1->data_type = DataType(FieldType::DOUBLE);
        param1->value = (double)12300000000000.34534534;
        std::shared_ptr<ConvertExpression> expr = std::make_shared<ConvertExpression>();
        expr->node_type = NodeType::CONVERT;
        expr->data_type = DataType(FieldType::INT64);
        expr->operand = param1;
        std::shared_ptr<DataField> df = expr->Calculate(table_->schema, table_->rows[0]);
        ASSERT_EQ(boost::any_cast<int64_t>(df->data), 12300000000000);
    }
    {
        std::shared_ptr<ParameterExpression> param1 = std::make_shared<ParameterExpression>();
        param1->node_type = NodeType::PARAMETER;
        param1->name = "K";
        std::shared_ptr<ConvertExpression> expr = std::make_shared<ConvertExpression>();
        expr->node_type = NodeType::CONVERT;
        expr->data_type = DataType(FieldType::INT16);
        expr->operand = param1;
        std::shared_ptr<DataField> df = expr->Calculate(table_->schema, table_->rows[0]);
        ASSERT_EQ(boost::any_cast<int16_t>(df->data), 1023);
    }
}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
