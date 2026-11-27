cmake_minimum_required(VERSION 3.11)
project(shm_pubsub VERSION 0.4.0 LANGUAGES CXX)

if(CMAKE_VERSION VERSION_LESS 3.21)
  if(CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
    set(PROJECT_IS_TOP_LEVEL ON)
  else()
    set(PROJECT_IS_TOP_LEVEL OFF)
  endif()
endif()

option(SHM_PUBSUB_BUILD_TESTS "Build shm_pubsub tests" ${PROJECT_IS_TOP_LEVEL})
option(SHM_PUBSUB_BUILD_EXAMPLES "Build shm_pubsub examples" ${PROJECT_IS_TOP_LEVEL})
option(SHM_PUBSUB_BUILD_TOOLS "Build the shmtop inspector" ${PROJECT_IS_TOP_LEVEL})
option(SHM_PUBSUB_BUILD_BENCHMARKS "Build reproducible benchmarks" OFF)
option(SHM_PUBSUB_INSTALL "Generate the install target" ${PROJECT_IS_TOP_LEVEL})

include(GNUInstallDirs)
find_package(Threads REQUIRED)

add_library(shm_pubsub INTERFACE)
add_library(shm_pubsub::shm_pubsub ALIAS shm_pubsub)
target_include_directories(shm_pubsub INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_compile_features(shm_pubsub INTERFACE cxx_std_11)
target_link_libraries(shm_pubsub INTERFACE Threads::Threads rt)

function(shm_pubsub_internal_target target)
  set_target_properties(${target} PROPERTIES CXX_STANDARD 11 CXX_STANDARD_REQUIRED ON
                                             CXX_EXTENSIONS OFF)
  target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic -Wshadow)
  target_link_libraries(${target} PRIVATE shm_pubsub)
endfunction()

if(SHM_PUBSUB_BUILD_EXAMPLES)
  foreach(example publisher subscriber broadcast work_queue wait_set)
    add_executable(${example}_example examples/${example}.cpp)
    shm_pubsub_internal_target(${example}_example)
  endforeach()
endif()

if(SHM_PUBSUB_BUILD_TOOLS)
  add_executable(shmtop tools/shmtop.cpp)
  shm_pubsub_internal_target(shmtop)
endif()

if(SHM_PUBSUB_BUILD_TESTS)
  enable_testing()
  set(SHM_PUBSUB_TESTS free_list_test mpmc_queue_test semantics_test multiprocess_test
                       fairness_test wakeup_test crash_leak_test crash_matrix_test stress_test
                       wait_set_test header_collision_test membership_test
                       slot_contention_test local_repair_test)
  foreach(test ${SHM_PUBSUB_TESTS})
    add_executable(${test} tests/${test}.cpp)
    shm_pubsub_internal_target(${test})
    add_test(NAME ${test} COMMAND ${test})
    set_tests_properties(${test} PROPERTIES TIMEOUT 60)
  endforeach()
  target_compile_definitions(crash_matrix_test PRIVATE SHM_PUBSUB_FAULT_INJECTION)
  target_compile_definitions(local_repair_test PRIVATE SHM_PUBSUB_FAULT_INJECTION)
  # Counts context switches, so CPU contention from parallel tests skews it.
  set_tests_properties(wakeup_test PROPERTIES RUN_SERIAL TRUE)
endif()

if(SHM_PUBSUB_BUILD_BENCHMARKS)
  foreach(bench latency_benchmark throughput_benchmark control_path_benchmark)
    add_executable(${bench} benchmarks/${bench}.cpp)
    shm_pubsub_internal_target(${bench})
  endforeach()
  add_executable(uds_benchmark benchmarks/uds_benchmark.cpp)
  set_target_properties(uds_benchmark PROPERTIES CXX_STANDARD 11)
  target_link_libraries(uds_benchmark PRIVATE Threads::Threads)
endif()

if(SHM_PUBSUB_INSTALL)
  include(CMakePackageConfigHelpers)
  set(SHM_PUBSUB_CMAKE_DIR ${CMAKE_INSTALL_LIBDIR}/cmake/shm_pubsub)
  install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
  install(TARGETS shm_pubsub EXPORT shm_pubsubTargets)
  install(EXPORT shm_pubsubTargets NAMESPACE shm_pubsub:: DESTINATION ${SHM_PUBSUB_CMAKE_DIR})
  configure_package_config_file(cmake/shm_pubsubConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/shm_pubsubConfig.cmake INSTALL_DESTINATION ${SHM_PUBSUB_CMAKE_DIR})
  set(SHM_PUBSUB_ARCH_INDEPENDENT)
  if(NOT CMAKE_VERSION VERSION_LESS 3.14)
    set(SHM_PUBSUB_ARCH_INDEPENDENT ARCH_INDEPENDENT)
  endif()
  # 0.x releases change the shared-memory layout between minor versions.
  write_basic_package_version_file(${CMAKE_CURRENT_BINARY_DIR}/shm_pubsubConfigVersion.cmake
    COMPATIBILITY SameMinorVersion ${SHM_PUBSUB_ARCH_INDEPENDENT})
  install(FILES ${CMAKE_CURRENT_BINARY_DIR}/shm_pubsubConfig.cmake
                ${CMAKE_CURRENT_BINARY_DIR}/shm_pubsubConfigVersion.cmake
          DESTINATION ${SHM_PUBSUB_CMAKE_DIR})
  install(FILES LICENSE NOTICE DESTINATION ${CMAKE_INSTALL_DATADIR}/doc/shm_pubsub)
endif()
