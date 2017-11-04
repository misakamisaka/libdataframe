cmake_minimum_required(VERSION 2.8)
project(aegir)

set(CMAKE_VERBOSE_MAKEFILE on)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g -pipe -W -Wall -fPIC --std=c++11")

set(WORKSPACE ${PROJECT_SOURCE_DIR}/../)

message(STATUS ${WORKSPACE})

include_directories(${WORKSPACE}/third/gflags/include
        ${WORKSPACE}/third/gtest/include ${WORKSPACE}/third/glog/include
        ${CMAKE_CURRENT_SOURCE_DIR}/src ${WORKSPACE}/third/boost/include
        ${CMAKE_BINARY_DIR} ${PROJECT_SOURCE_DIR}/include)

link_directories(${WORKSPACE}/third/gflags/lib
        ${WORKSPACE}/third/glog/include
        ${WORKSPACE}/third/gtest/lib ${WORKSPACE}/third/boost/lib)

add_subdirectory(src)
add_subdirectory(test)
