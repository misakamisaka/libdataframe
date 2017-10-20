#include "expression/aggregate_expression.h"
#include <glog/logging.h>
#include "column.h"
#include "expression/expression_exception.h"
#include "row.h"
#include "schema.h"
#include "type/type.h"
#include "type/type_converter.h"

using std::make_shared;

namespace mortred {
namespace expression {
void AggregateExpression::Resolve(const std::shared_ptr<Schema>& schema) {
  LeafExpression::Resolve(schema);
  index_ = schema->GetIndexByName(column_name_);
  data_type_ = schema->GetColumnByIndex(index_)->data_type();
  ASSERT_DATA_TYPE(data_type_, Type::LIST)
  nullable_ = std::static_pointer_cast<ListType>(data_type_)->value_column()->nullable();
  data_type_ = std::static_pointer_cast<ListType>(data_type_)->value_column()->data_type();
}
std::shared_ptr<DataField> MaxExpr::Eval(const std::shared_ptr<Row>& row) const {
  const auto& cell_vec = boost::any_cast<std::vector<std::shared_ptr<Cell>>>(row->at(index_)->value());
  return std::make_shared<DataField>(*(std::max_element(cell_vec.begin(), cell_vec.end(),
                        [this](const std::shared_ptr<Cell>& largest, const std::shared_ptr<Cell>& current) {
                          return DataField(largest, data_type_).LessThan(DataField(current, data_type_));
                        })), data_type_);
}
std::shared_ptr<DataField> MinExpr::Eval(const std::shared_ptr<Row>& row) const {
  const auto& cell_vec = boost::any_cast<std::vector<std::shared_ptr<Cell>>>(row->at(index_)->value());
  return std::make_shared<DataField>(*(std::min_element(cell_vec.begin(), cell_vec.end(),
                        [this](const std::shared_ptr<Cell>& current, const std::shared_ptr<Cell>& smallest) {
                          return DataField(current, data_type_).LessThan(DataField(smallest, data_type_));
                        })), data_type_);
}
std::shared_ptr<DataField> FirstExpr::Eval(const std::shared_ptr<Row>& row) const {
  const auto& cell_vec = boost::any_cast<std::vector<std::shared_ptr<Cell>>>(row->at(index_)->value());
  return std::make_shared<DataField>(*cell_vec.begin(), data_type_);
}
std::shared_ptr<DataField> LastExpr::Eval(const std::shared_ptr<Row>& row) const {
  const auto& cell_vec = boost::any_cast<std::vector<std::shared_ptr<Cell>>>(row->at(index_)->value());
  return std::make_shared<DataField>(*cell_vec.rbegin(), data_type_);
}
void AvgExpr::Resolve(const std::shared_ptr<Schema>& schema) {
  AggregateExpression::Resolve(schema);
  cell_data_type_ = data_type_;
  data_type_ = DataTypes::MakeDoubleType();
  nullable_ = false;
}
std::shared_ptr<DataField> AvgExpr::Eval(const std::shared_ptr<Row>& row) const {
  const auto& cell_vec = boost::any_cast<std::vector<std::shared_ptr<Cell>>>(row->at(index_)->value());
  if (cell_vec.size() == 0) {
    return std::make_shared<DataField>(std::make_shared<Cell>(false, 0), data_type_);
  }
  double result = 0;
  std::shared_ptr<DataField> data_field;
  for (const auto& it : cell_vec) {
    data_field = type_cast(data_type_, std::make_shared<DataField>(it, cell_data_type_));
    if (!data_field->cell()->is_null()) {
      result += boost::any_cast<double>(data_field->cell()->value());
    }
  }
  return std::make_shared<DataField>(std::make_shared<Cell>(false, result / cell_vec.size()), data_type_);
}
void SumExpr::Resolve(const std::shared_ptr<Schema>& schema) {
  AggregateExpression::Resolve(schema);
  cell_data_type_ = data_type_;
  if (DataTypes::IsInteger(data_type_->type)) {
    data_type_ = DataTypes::MakeInt64Type();
  } else if (DataTypes::IsFloating(data_type_->type)) {
    data_type_ = DataTypes::MakeDoubleType();
  } else {
      throw ExpressionException("AggregateExpression resolve error[data_type is not primitive]");
  }
  nullable_ = false;
}
std::shared_ptr<DataField> SumExpr::Eval(const std::shared_ptr<Row>& row) const {
  const auto& cell_vec = boost::any_cast<std::vector<std::shared_ptr<Cell>>>(row->at(index_)->value());
  if (data_type_->type == Type::INT64){
    int64_t result = 0;
    std::shared_ptr<DataField> data_field;
    for (const auto& it : cell_vec) {
      data_field = type_cast(data_type_, std::make_shared<DataField>(it, cell_data_type_));
      if (!data_field->cell()->is_null()) {
        result += boost::any_cast<int64_t>(data_field->cell()->value());
      }
    }
    return std::make_shared<DataField>(std::make_shared<Cell>(false, result), data_type_);
  } else if (data_type_->type == Type::DOUBLE) {
    double result = 0;
    std::shared_ptr<DataField> data_field;
    for (const auto& it : cell_vec) {
      data_field = type_cast(data_type_, std::make_shared<DataField>(it, cell_data_type_));
      if (!data_field->cell()->is_null()) {
        result += boost::any_cast<double>(data_field->cell()->value());
      }
    }
    return std::make_shared<DataField>(std::make_shared<Cell>(false, result), data_type_);
  } else {
    throw ExpressionException("AggregateExpression resolve error[data_type is not primitive]");
  }
}
void CountExpr::Resolve(const std::shared_ptr<Schema>& schema) {
  AggregateExpression::Resolve(schema);
  data_type_ = DataTypes::MakeInt64Type();
  nullable_ = false;
}
std::shared_ptr<DataField> CountExpr::Eval(const std::shared_ptr<Row>& row) const {
  const auto& cell_vec = boost::any_cast<std::vector<std::shared_ptr<Cell>>>(row->at(index_)->value());
  return std::make_shared<DataField>(std::make_shared<Cell>(false, (int64_t)cell_vec.size()), data_type_);
}
}
}
