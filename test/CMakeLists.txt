cmake_minimum_required(VERSION 2.8)

ADD_EXECUTABLE(test_type test_type.cpp)
TARGET_LINK_LIBRARIES(test_type dataframe libgtest.a 
dl pthread)
ADD_EXECUTABLE(test_expression test_expression.cpp)
TARGET_LINK_LIBRARIES(test_expression dataframe libgtest.a dl pthread)
