cmake_minimum_required(VERSION 3.11)
project(shm_pubsub_package_consumer CXX)

set(SHM_PUBSUB_SOURCE_DIR "" CACHE PATH "Use add_subdirectory instead of find_package")
if(SHM_PUBSUB_SOURCE_DIR)
  add_subdirectory(${SHM_PUBSUB_SOURCE_DIR} shm_pubsub)
else()
  find_package(shm_pubsub 0.4 REQUIRED)
endif()

add_executable(package_consumer main.cpp)
target_link_libraries(package_consumer PRIVATE shm_pubsub::shm_pubsub)
