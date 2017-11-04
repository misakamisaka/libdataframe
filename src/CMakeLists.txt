cmake_minimum_required(VERSION 2.8)

AUX_SOURCE_DIRECTORY(. SRC)

ADD_LIBRARY(dataframe type/type.cpp type/type_visitor.cpp
expression/alias_expression.cpp
expression/expression.cpp expression/arithmetic_expression.cpp
expression/predicate_expression.cpp column.cpp
schema.cpp data_field.cpp data_frame.cpp
expression/aggregate_expression.cpp
expression/type_cast_expression.cpp)
TARGET_LINK_LIBRARIES(dataframe libgtest.a 
libglog.a libgflags.a dl librt.a libz.a dl pthread)

INSTALL(TARGETS dataframe LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)
INSTALL(DIRECTORY ${PROJECT_SOURCE_DIR}/include/ DESTINATION include
FILES_MATCHING PATTERN "*")
