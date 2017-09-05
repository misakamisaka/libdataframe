#ifndef MORTRED_EXPRESSION_H
#define MORTRED_EXPRESSION_H

#include <map>
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
    PAIR,
    POWER,              // **
    RIGHT_SHIFT,        // >>
    SUB,                // -
};

const static std::map<NodeType, std::string> node_type2str_map = 
{
  {NodeType::ADD, "+"},
  {NodeType::AND_ALSO, "&&"},
  {NodeType::BIT_AND, "&"},
  {NodeType::BIT_OR, "|"},
  {NodeType::BIT_NOT, "~"},
  {NodeType::BIT_XOR, "^"},
  {NodeType::DEC, "--"},
  {NodeType::EQ, "=="},
  {NodeType::GT, ">"},
  {NodeType::GE, ">="},
  {NodeType::INC, "++"},
  {NodeType::LEFT_SHIFT, "<<"},
  {NodeType::LT, "<"},
  {NodeType::LE, "<="},
  {NodeType::MOD, "%"},
  {NodeType::MUL, "*"},
  {NodeType::NEG, "-"},
  {NodeType::NOT, "!"},
  {NodeType::NE, "!="},
  {NodeType::OR_ELSE, "||"},
  {NodeType::RIGHT_SHIFT, ">>"},
  {NodeType::SUB, "-"}
};

class Expression {
 public:
  explicit Expression(NodeType node_type) : node_type_(node_type) {}
  virtual void Resolve(std::shared_ptr<Schema> schema) = 0;
  virtual std::shared_ptr<DataField> Eval(const std::shared_ptr<Row>& row) const = 0;
  virtual std::vector<std::shared_ptr<Expression>> GetChildren() = 0;
  virtual std::string ToString() = 0;
  bool nullable() const { return nullable_; }
  std::shared_ptr<DataType> data_type() const { return data_type_; }
  NodeType node_type() const { return node_type_; }
 protected:
  bool nullable_;
  std::shared_ptr<DataType> data_type_;
  bool resolved_;
  NodeType node_type_;
};

class LeafExpression : public Expression {
 public:
  using Expression::Expression;
  virtual void Resolve(std::shared_ptr<Schema> schema);
//  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row);
  virtual std::vector<std::shared_ptr<Expression>> GetChildren() {
    return std::vector<std::shared_ptr<Expression>>();
  }
//  virtual std::string ToString();
};

class UnaryExpression : public Expression {
 public:
  explicit UnaryExpression(std::shared_ptr<Expression> child, NodeType node_type)
    :Expression(node_type), child_(child) {}
  virtual void Resolve(std::shared_ptr<Schema> schema);
//  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row);
  virtual std::vector<std::shared_ptr<Expression>> GetChildren() {
    return {child_};
  }
  std::shared_ptr<Expression> child() const { return child_; }
//  virtual std::string ToString();
 protected:
  std::shared_ptr<Expression> child_;
};

class BinaryExpression : public Expression {
 public:
   BinaryExpression(std::shared_ptr<Expression> left,
       std::shared_ptr<Expression> right,
       NodeType node_type)
     :Expression(node_type), left_(left), right_(right) {}
  virtual void Resolve(std::shared_ptr<Schema> schema);
//  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row);
  virtual std::vector<std::shared_ptr<Expression>> GetChildren() {
    return {left_, right_};
  }
  std::shared_ptr<Expression> left() const { return left_; }
  std::shared_ptr<Expression> right() const { return right_; }
//  virtual std::string ToString();
 protected:
  std::shared_ptr<Expression> left_;
  std::shared_ptr<Expression> right_;
};

class TenaryExpression : public Expression {
 public:
   TenaryExpression(std::shared_ptr<Expression> child1,
       std::shared_ptr<Expression> child2,
       std::shared_ptr<Expression> child3,
       NodeType node_type)
     :Expression(node_type), child1_(child1), child2_(child2), child3_(child3) {}
  virtual void Resolve(std::shared_ptr<Schema> schema);
//  virtual std::shared_ptr<DataField> Eval(std::shared_ptr<Row> row);
  virtual std::vector<std::shared_ptr<Expression>> GetChildren() {
    return {child1_, child2_, child3_};
  }
  std::shared_ptr<Expression> child1() const { return child1_; }
  std::shared_ptr<Expression> child2() const { return child2_; }
  std::shared_ptr<Expression> child3() const { return child3_; }
//  virtual std::string ToString();
 protected:
  std::shared_ptr<Expression> child1_;
  std::shared_ptr<Expression> child2_;
  std::shared_ptr<Expression> child3_;
};

class PairExpression : public BinaryExpression {
 public:
   PairExpression(std::shared_ptr<Expression> left,
       std::shared_ptr<Expression> right)
     :BinaryExpression(left, right, NodeType::PAIR) {}
  virtual std::shared_ptr<DataField> Eval(const std::shared_ptr<Row>& row) const;
  virtual std::string ToString();
};

class ArrayExpression : public Expression {
 public:
  explicit ArrayExpression(std::vector<std::shared_ptr<Expression>> children)
  :Expression(NodeType::ARRAY), children_(children) { }
  virtual void Resolve(std::shared_ptr<Schema> schema);
  virtual std::shared_ptr<DataField> Eval(const std::shared_ptr<Row>& row) const;
  virtual std::vector<std::shared_ptr<Expression>> GetChildren() {
    return children_;
  }
  virtual std::string ToString();
 protected:
  std::vector<std::shared_ptr<Expression>> children_;
};

} //namespace expression
} //namespace mortred
#endif //MORTRED_EXPRESSION_H
