cmake_minimum_required(VERSION 3.10.0)
project(lockfree_shm VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")

add_executable(publisher publisher.cpp)
target_link_libraries(publisher atomic)

add_executable(subscriber subscriber.cpp)
target_link_libraries(subscriber atomic)

enable_testing()
add_executable(free_list_test tests/free_list_test.cpp)
target_include_directories(free_list_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(free_list_test atomic)
add_test(NAME free_list_test COMMAND free_list_test)
add_executable(atomic_queue_test tests/atomic_queue_test.cpp)
target_include_directories(atomic_queue_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(atomic_queue_test atomic)
add_test(NAME atomic_queue_test COMMAND atomic_queue_test)
