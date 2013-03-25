cmake_minimum_required(VERSION 2.8)
project(worddictlib CXX)

include_directories("/usr/include/python2.7")
SET(SRC_LIST main.cpp)
ADD_LIBRARY (worddict SHARED ${SRC_LIST})
TARGET_LINK_LIBRARIES (worddict boost_python db)
