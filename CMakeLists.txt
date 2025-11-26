cmake_minimum_required(VERSION 3.10.0)
project(lockfree_shm VERSION 0.1.0 LANGUAGES C CXX)

add_executable(publisher publisher.cpp)
add_executable(subscriber subscriber.cpp)

