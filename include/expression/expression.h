#ifndef MORTRED_EXPRESSION_H
#define MORTRED_EXPRESSION_H

#include <memory>
#include <string>
#include <vector>
#include "cell.h"
#include "data_field.h"
#include "data_frame.h"
#include "expression_exception.h"
#include "schema.h"

namespace mortred {
namespace expression {

enum class NodeType {
    ADD,                // +
    ALIAS,
    AND_ALSO,           // &&
    ARRAY,
    BIT_AND,            // &
    BIT_OR,             // |
    BIT_NOT,            // ~
    BIT_XOR,                // ^
    COLUMN,
    CONDITIONAL,        // ?:
    CONSTANT,
    CONVERT,
    DEC,                // --
    DIV,                // /
    EVENT_TYPE,
    EQ,                 // ==
    GT,                 // >
    GE,                 // >=
    IN,
    INC,                //++
    IS_FALSE,
    IS_NULL,
    IS_TRUE,
    LEFT_SHIFT,         // <<
    LT,                 // <
    LE,                 // <=
    METHOD,
    MOD,                // %
    MUL,                // *
    NEG,                // -
    NOT,                // !
    NOT_IN,
    NE,                 // !=
    OR_ELSE,            // ||
    POWER,              // **
    RIGHT_SHIFT,        // >>
    SUB,                // -
};

class Expression {
 public:
  virtual void Resolve(std::shared_ptr<Schema> schema) = 0;
  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row) = 0;
  virtual std::vector<std::shared_ptr<Expression>> GetChildren() = 0;
  virtual std::string ToString() = 0;
  bool nullable() { return nullable_; }
  std::shared_ptr<DataType> data_type() { return data_type_; }
 protected:
  bool nullable_;
  std::shared_ptr<DataType> data_type_;
};

class LeafExpression : public Expression {
};

class UnaryExpression : public Expression {
 public:
  explicit UnaryExpression(std::shared_ptr<Expression> child) : child_(child) {}
  virtual void Resolve(std::shared_ptr<Schema> schema);
  virtual std::vector<std::shared_ptr<Expression>> GetChildren() {
    return {child_};
  }
 protected:
  std::shared_ptr<Expression> child_;
};

class BinaryExpression : public Expression {
 public:
   BinaryExpression(std::shared_ptr<Expression> left,
       std::shared_ptr<Expression> right)
     :left_(left), right_(right) {}
  virtual void Resolve(std::shared_ptr<Schema> schema);
  virtual std::vector<std::shared_ptr<Expression>> GetChildren() {
    return {left_, right_};
  }
 protected:
  std::shared_ptr<Expression> left_;
  std::shared_ptr<Expression> right_;
};

class TenaryExpression : public Expression {
 public:
   TenaryExpression(std::shared_ptr<Expression> child1,
       std::shared_ptr<Expression> child2,
       std::shared_ptr<Expression> child3)
     :child1_(child1), child2_(child2), child3_(child3) {}
  virtual void Resolve(std::shared_ptr<Schema> schema);
  virtual std::vector<std::shared_ptr<Expression>> GetChildren() {
    return {child1_, child2_, child3_};
  }
 protected:
  std::shared_ptr<Expression> child1_;
  std::shared_ptr<Expression> child2_;
  std::shared_ptr<Expression> child3_;
};

class ArrayExpression : public Expression {
 public:
  explicit ArrayExpression(std::vector<std::shared_ptr<Expression>> children) : children_(children) {}
  virtual void Resolve(std::shared_ptr<Schema> schema);
  virtual std::vector<std::shared_ptr<Expression>> GetChildren() {
    return children_;
  }
 protected:
  std::vector<std::shared_ptr<Expression>> children_;
};

} //namespace expression
} //namespace mortred
#endif //MORTRED_EXPRESSION_H
