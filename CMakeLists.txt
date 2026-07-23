cmake_minimum_required(VERSION 3.10.0)
project(lockfree_shm VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
find_package(Threads REQUIRED)

add_executable(publisher publisher.cpp)
target_link_libraries(publisher PRIVATE atomic Threads::Threads)

add_executable(subscriber subscriber.cpp)
target_link_libraries(subscriber PRIVATE atomic Threads::Threads)

enable_testing()
add_executable(free_list_test tests/free_list_test.cpp)
target_include_directories(free_list_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(free_list_test PRIVATE atomic Threads::Threads)
add_test(NAME free_list_test COMMAND free_list_test)
add_executable(atomic_queue_test tests/atomic_queue_test.cpp)
target_include_directories(atomic_queue_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(atomic_queue_test PRIVATE atomic Threads::Threads)
add_test(NAME atomic_queue_test COMMAND atomic_queue_test)
