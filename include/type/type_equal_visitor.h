#ifndef MORTRED_TYPE_EQUAL_VISITOR_H
#define MORTRED_TYPE_EQUAL_VISITOR_H

#include "type.h"

namespace mortred {

class TypeEqualsVisitor {
 public:
  explicit TypeEqualsVisitor(const DataType& right) : right_(right), result_(false) {}

  void VisitChildren(const DataType& left) {
    if (left.num_children() != right_.num_children()) {
      result_ = false;
      return;
    }

    for (int i = 0; i < left.num_children(); ++i) {
      if (!left.child(i)->Equals(right_.child(i))) {
        result_ = false;
        return;
      }
    }
    result_ = true;
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<NoExtraMeta, T>::value ||
                            std::is_base_of<PrimitiveCType, T>::value,
      void>::type
  Visit(const T&) {
    result_ = true;
  }

  void Visit(const TimestampType&) {
    result_ = true;
  }

  void Visit(const IntervalType& left) {
    const auto& right = static_cast<const IntervalType&>(right_);
    result_ = left.unit == right.unit;
  }

  void Visit(const DecimalType& left) {
    const auto& right = static_cast<const DecimalType&>(right_);
    result_ = left.precision == right.precision && left.scale == right.scale;
  }

  void Visit(const ListType& left) { VisitChildren(left); }

  void Visit(const StructType& left) { VisitChildren(left); }

  void Visit(const MapType& left) {
    const auto& right = static_cast<const MapType&>(right_);
    result_ = left.key_type()->Equals(right.key_type()) &&
              left.value_type()->Equals(right.value_type());
  }

  bool result() const { return result_; }

 protected:
  const DataType& right_;
  bool result_;
};

}

#endif
