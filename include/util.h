#ifndef MORTRED_UTIL_H
#define MORTRED_UTIL_H

#ifndef DISALLOW_COPY_AND_ASSIGN
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&) = delete;      \
  TypeName& operator=(const TypeName&) = delete
#endif

#define ASSERT_EXPR_TYPE(expr, expected_node_type)                        \
  if (expr->node_type() != expected_node_type) {                          \
    LOG(INFO) << "not supported expression type";                         \
    throw DataFrameException("not supported expression type");            \
  }

#endif
