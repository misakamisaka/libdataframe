#ifndef MORTRED_AGGREGATE_EXPRESSION_H
#define MORTRED_AGGREGATE_EXPRESSION_H

#include "expression.h"

namespace mortred {
namespace expression {

class MaxExpr : public UnaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row) const;
  virtual std::string ToString() {
    std::string ret;
    return ret;
  }
};

class MinExpr : public UnaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row) const;
  virtual std::string ToString() {
    std::string ret;
    return ret;
  }
};

class FirstExpr : public UnaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row) const;
  virtual std::string ToString() {
    std::string ret;
    return ret;
  }
};
class LastExpr : public UnaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row) const;
  virtual std::string ToString() {
    std::string ret;
    return ret;
  }
};

//data type double
class AvgExpr : public UnaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row) const;
  virtual std::string ToString() {
    std::string ret;
    return ret;
  }
};
//data type double
class SumExpr : public UnaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row) const;
  virtual std::string ToString() {
    std::string ret;
    return ret;
  }
};

//data type int
class CountExpr : public UnaryExpression {
 public:
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row) const;
  virtual std::string ToString() {
    std::string ret;
    return ret;
  }
};

} //namespace expression
} //namespace mortred
#endif //MORTRED_AGGREGATE_EXPRESSION_H
