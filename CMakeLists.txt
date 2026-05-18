cmake_minimum_required(VERSION 3.10.0)
project(lockfree_shm VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")

add_executable(publisher publisher.cpp)
target_link_libraries(publisher atomic)

add_executable(subscriber subscriber.cpp)
target_link_libraries(subscriber atomic)