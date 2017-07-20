#define GTEST_USE_OWN_TR1_TUPLE 0
#include <gtest/gtest.h>
#include <glog/logging.h>
#include "type/type.h"

using namespace mortred;

class TypeTest : public testing::Test {
public:
    TypeTest() {
    }
protected:
    virtual void SetUp() {
    }
public:
};

TEST_F(TypeTest, test_type) {
  ASSERT_TRUE(DataTypes::MakeNullType()->Equals(DataTypes::MakeNullType()));
  ASSERT_TRUE(DataTypes::MakeBooleanType()->Equals(DataTypes::MakeBooleanType()));
  ASSERT_TRUE(DataTypes::MakeUInt8Type()->Equals(DataTypes::MakeUInt8Type()));
  ASSERT_TRUE(DataTypes::MakeInt8Type()->Equals(DataTypes::MakeInt8Type()));
  ASSERT_TRUE(DataTypes::MakeUInt16Type()->Equals(DataTypes::MakeUInt16Type()));
  ASSERT_TRUE(DataTypes::MakeInt16Type()->Equals(DataTypes::MakeInt16Type()));
  ASSERT_TRUE(DataTypes::MakeUInt32Type()->Equals(DataTypes::MakeUInt32Type()));
  ASSERT_TRUE(DataTypes::MakeInt32Type()->Equals(DataTypes::MakeInt32Type()));
  ASSERT_TRUE(DataTypes::MakeUInt64Type()->Equals(DataTypes::MakeUInt64Type()));
  ASSERT_TRUE(DataTypes::MakeInt64Type()->Equals(DataTypes::MakeInt64Type()));
  ASSERT_TRUE(DataTypes::MakeFloatType()->Equals(DataTypes::MakeFloatType()));
  ASSERT_TRUE(DataTypes::MakeDoubleType()->Equals(DataTypes::MakeDoubleType()));
  ASSERT_TRUE(DataTypes::MakeDecimalType(2, 1)->Equals(DataTypes::MakeDecimalType(2, 1)));
  ASSERT_TRUE(DataTypes::MakeTimestampType()->Equals(DataTypes::MakeTimestampType()));
  ASSERT_TRUE(DataTypes::MakeIntervalType(IntervalUnit::MONTH)->Equals(DataTypes::MakeIntervalType(IntervalUnit::MONTH)));
  ASSERT_TRUE(DataTypes::MakeMapType(DataTypes::MakeInt16Type(), DataTypes::MakeDoubleType())->Equals(DataTypes::MakeMapType(DataTypes::MakeInt16Type(), DataTypes::MakeDoubleType())));
  ASSERT_TRUE(DataTypes::MakeListType(DataTypes::MakeMapType(DataTypes::MakeInt16Type(), DataTypes::MakeDoubleType()))->Equals(DataTypes::MakeListType(DataTypes::MakeMapType(DataTypes::MakeInt16Type(), DataTypes::MakeDoubleType()))));
}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
