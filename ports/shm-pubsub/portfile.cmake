# Overlay port: the sources are this repository, not a downloaded archive.
set(SOURCE_PATH "${CMAKE_CURRENT_LIST_DIR}/../..")

file(COPY "${SOURCE_PATH}/include/shm_pubsub" DESTINATION "${CURRENT_PACKAGES_DIR}/include")
file(INSTALL "${SOURCE_PATH}/LICENSE" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}" RENAME copyright)

file(WRITE "${CURRENT_PACKAGES_DIR}/share/${PORT}/shm-pubsub-config.cmake"
"include(CMakeFindDependencyMacro)
find_dependency(Threads)
if(NOT TARGET shm_pubsub::shm_pubsub)
  add_library(shm_pubsub::shm_pubsub INTERFACE IMPORTED)
  set_target_properties(shm_pubsub::shm_pubsub PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES \"\${CMAKE_CURRENT_LIST_DIR}/../../include\"
    INTERFACE_LINK_LIBRARIES \"Threads::Threads;rt\")
endif()
")
set(VCPKG_POLICY_EMPTY_INCLUDE_FOLDER enabled)
