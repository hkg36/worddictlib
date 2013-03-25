project(worddictlib)

include_directories("/usr/include/python2.7")
ADD_LIBRARY (worddict SHARED main.cpp)
TARGET_LINK_LIBRARIES (worddict boost_python db)
