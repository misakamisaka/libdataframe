#ifndef MORTRED_UTIL_H
#define MORTRED_UTIL_H

#include <glog/logging.h>
#include "data_frame_exception.h"

#ifndef DISALLOW_COPY_AND_ASSIGN
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&) = delete;      \
  TypeName& operator=(const TypeName&) = delete
#endif

#define ASSERT_EXPR_TYPE(expr, expected_node_type)                        \
  if (expr->node_type() != expected_node_type) {                          \
    LOG(INFO) << "unexpected expression type";                         \
    throw DataFrameException("not supported expression type");            \
  }

#define ASSERT_DATA_TYPE(data_type, expected_data_type)                        \
  if (data_type->type != expected_data_type) {                          \
    LOG(INFO) << "unexpected data type";                         \
    throw DataFrameException("not supported expression type");            \
  }

#endif
