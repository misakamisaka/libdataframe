cmake_minimum_required(VERSION 2.8)

AUX_SOURCE_DIRECTORY(. SRC)

ADD_LIBRARY(dataframe type/type.cpp type/type_visitor.cpp
expression/expression.cpp expression/arithmetic_expression.cpp column.cpp)
TARGET_LINK_LIBRARIES(dataframe libgtest.a 
libglog.a libgflags.a dl librt.a libz.a dl pthread)
