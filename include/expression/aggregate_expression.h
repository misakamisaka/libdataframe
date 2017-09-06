#ifndef MORTRED_AGGREGATE_EXPRESSION_H
#define MORTRED_AGGREGATE_EXPRESSION_H

#include "expression.h"

namespace mortred {
namespace expression {

class AggregateExpression : public LeafExpression {
 public:
  AggregateExpression(const std::string& agg_method_name, const std::string& column_name)
    : LeafExpression(NodeType::AGG),
    column_name_(column_name),
    agg_method_name_(agg_method_name) { }
  virtual void Resolve(const std::shared_ptr<Schema>& schema);
  virtual std::string ToString() {
    std::string ret;
    ret += agg_method_name_;
    ret += "(";
    ret += column_name_;
    ret += ")";
    return ret;
  }
  std::string column_name() const { return column_name_; }
  int index() const { return index_; }
 protected:
  std::string column_name_;
  int index_{-1};
  std::string agg_method_name_;
};

class MaxExpr : public AggregateExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(const std::shared_ptr<Row>& row) const;
};

class MinExpr : public AggregateExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(const std::shared_ptr<Row>& row) const;
};

class FirstExpr : public AggregateExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(const std::shared_ptr<Row>& row) const;
};

class LastExpr : public AggregateExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(const std::shared_ptr<Row>& row) const;
};

//data type double
class AvgExpr : public AggregateExpression { 
 public:
  virtual void Resolve(const std::shared_ptr<Schema>& schema);
  virtual std::shared_ptr<DataField> Eval(const std::shared_ptr<Row>& row) const;
 private:
  std::shared_ptr<DataType> cell_data_type_;
};

//data type long or double
class SumExpr : public AggregateExpression {
 public:
  virtual void Resolve(const std::shared_ptr<Schema>& schema);
  virtual std::shared_ptr<DataField> Eval(const std::shared_ptr<Row>& row) const;
 private:
  std::shared_ptr<DataType> cell_data_type_;
};

//data type long
class CountExpr : public AggregateExpression {
 public:
  virtual void Resolve(const std::shared_ptr<Schema>& schema);
  virtual std::shared_ptr<DataField> Eval(const std::shared_ptr<Row>& row) const;
};

} //namespace expression
} //namespace mortred
#endif //MORTRED_AGGREGATE_EXPRESSION_H
