cmake_minimum_required(VERSION 3.2.1)
# set(CMAKE_C_STANDARD 17)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_EXTENSIONS OFF)

# set(CMAKE_C_COMPILER g++-11)
set(CMAKE_CXX_COMPILER g++-11)
# set(CMAKE_VERBOSE_MAKEFILE ON)

# Set the output folder where your program will be created
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_DEBUG ${CMAKE_BINARY_DIR}/Debug/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_DEBUG ${CMAKE_BINARY_DIR}/Debug/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_BINARY_DIR}/Debug/bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELEASE ${CMAKE_BINARY_DIR}/Release/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_RELEASE ${CMAKE_BINARY_DIR}/Release/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_BINARY_DIR}/Release/bin)
# set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_BUILD_TYPE Release)

# Set custom optimization flags
set(CMAKE_CXX_FLAGS_RELEASE "-O3") # Use -O2 optimization for release builds
set(CMAKE_CXX_FLAGS_DEBUG "-g -Og")   # Use -Og optimization for debug builds
add_compile_options(-Wall -Wextra)

# set(DEBUG TRUE)
# set(DEBUG FALSE)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${DEBUG_OPT} -fopenmp -march=native")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${DEBUG_OPT} -fopenmp -march=native")



set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(INSTALL_GTEST OFF)

project(VertexSurge VERSION 1.0)

install(
    FILES "${CMAKE_BINARY_DIR}/compile_commands.json"
    DESTINATION "${CMAKE_SOURCE_DIR}"
)



# if (DEBUG)
#     message(STATUS "Debug mode")
#     set(DEBUG_OPT "-g")
# else() 
#     message(STATUS "Release mode")
#     set (DEBUG_OPT "-O3")
# endif()





set(SRC_INCLUDE ${PROJECT_SOURCE_DIR}/src/include)
set(COMMON_INCLUDE_DIRS ${PROJECT_SOURCE_DIR}/src/objects/vql ${SRC_INCLUDE} ${SRC_INCLUDE}/common ${SRC_INCLUDE}/utils)

# header only json
include_directories(third_party)
include_directories(/usr/local/include/antlr4-runtime)
include_directories(${COMMON_INCLUDE_DIRS})

include(FetchContent)

# fmtlib
find_package(fmt 9.1.0 QUIET)

if(NOT fmt_FOUND)
    message(STATUS "fmt not found, using FetchContent")
    FetchContent_Declare(
        fmtlib
        GIT_REPOSITORY https://github.com/fmtlib/fmt.git
        GIT_TAG 9.1.0 # specify the exact version
    )
    FetchContent_MakeAvailable(fmtlib)
endif()



# spdlog
find_package(spdlog 1.11.0 QUIET)

if(NOT spdlog_FOUND)
    message(STATUS "spdlog not found, using FetchContent")
    FetchContent_Declare(
        spdlog
        GIT_REPOSITORY https://github.com/gabime/spdlog.git
        GIT_TAG v1.11.0 # specify the version you want
    )
    FetchContent_MakeAvailable(spdlog)
endif()



find_package(xxHash QUIET)
if(NOT xxHash_FOUNG)
    message(STATUS "xxHash not found, using FetchContent")
    FetchContent_Declare(
        xxHash
        GIT_REPOSITORY https://github.com/Cyan4973/xxHash.git
        GIT_TAG v0.8.0
    )
    FetchContent_MakeAvailable(xxHash)
    include_directories(${xxHash_SOURCE_DIR})
    # message(STATUS "xxHash_SOURCE_DIR: ${xxHash_SOURCE_DIR}")
endif()



include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://codeload.github.com/google/googletest/zip/refs/tags/v1.14.0
)
FetchContent_MakeAvailable(googletest)

enable_testing()



include(GoogleTest)
add_subdirectory(src/objects)
add_subdirectory(src/test)


set(INSTALL_DIR ${CMAKE_CURRENT_SOURCE_DIR}/bin)
message(STATUS "INSTALL_DIR: ${INSTALL_DIR}")






# Collect all currently added targets in all subdirectories
#
# Parameters:
# - _result the list containing all found targets
# - _dir root directory to start looking from
function(get_all_targets _result _dir)
    get_property(_subdirs DIRECTORY "${_dir}" PROPERTY SUBDIRECTORIES)

    foreach(_subdir IN LISTS _subdirs)
        get_all_targets(${_result} "${_subdir}")
    endforeach()

    get_directory_property(_sub_targets DIRECTORY "${_dir}" BUILDSYSTEM_TARGETS)
    set(${_result} ${${_result}} ${_sub_targets} PARENT_SCOPE)
endfunction()


get_all_targets(ALL_TARGETS ${CMAKE_CURRENT_LIST_DIR})

set(SPECIFIC_TARGETS_TO_REMOVE
    "base"
    "gtest"
    "gtest_main"
    "gmock"
    "gmock_main"
    "vql"
)

list(REMOVE_ITEM ALL_TARGETS ${SPECIFIC_TARGETS_TO_REMOVE})


message(STATUS "ALL_TARGETS: ${ALL_TARGETS}")

# Install all executable targets
install(TARGETS ${ALL_TARGETS}
    RUNTIME DESTINATION ${INSTALL_DIR})


file(GLOB_RECURSE SOURCE_FILES
    ${CMAKE_SOURCE_DIR}/src/*.cpp
    ${CMAKE_SOURCE_DIR}/src/*.hh
    ${CMAKE_SOURCE_DIR}/src/*.h)

add_custom_target(
    fmt
    COMMAND clang-format -i -style=file ${SOURCE_FILES}
    COMMENT "Running clang-format on source files"
)




