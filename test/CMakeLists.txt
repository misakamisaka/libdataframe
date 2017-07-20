cmake_minimum_required(VERSION 2.8)

AUX_SOURCE_DIRECTORY(. SRC)
ADD_EXECUTABLE(test_type test_type.cpp)
TARGET_LINK_LIBRARIES(test_type dataframe libgtest.a 
dl pthread)
