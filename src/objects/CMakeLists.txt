# add_subdirectory(testify)

# Function to add a library
set(SOURCE_DIRS
    "meta"
    "storage"
    "execution"
    "operator"
    # "vql"
    "plan"
)

# Initialize an empty list to store all .cpp files
set(ALL_CPP_FILES "")

# Loop through each source directory
foreach(SOURCE_DIR ${SOURCE_DIRS})
    # Find all .cpp files in the current source directory
    file(GLOB CPP_FILES_IN_DIR "${CMAKE_CURRENT_SOURCE_DIR}/${SOURCE_DIR}/*.cpp")

    # Append the list of .cpp files to the overall list
    list(APPEND ALL_CPP_FILES ${CPP_FILES_IN_DIR})
endforeach()

set(SPECIFIC_FILES_TO_REMOVE
    "${CMAKE_CURRENT_SOURCE_DIR}/operator/case1.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/operator/case3.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/operator/case5.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/operator/case6.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/vql/vql-interface.cpp"
    # Add more files to remove as needed
)
# message(STATUS "ALL: ${ALL_CPP_FILES}")

# Remove specific files from the overall list
list(REMOVE_ITEM ALL_CPP_FILES ${SPECIFIC_FILES_TO_REMOVE})

add_library(base STATIC ${ALL_CPP_FILES})
# target_link_libraries(base  GTest::gtest_main fmt::fmt tbb tbb xxhash vql antlr4-runtime)
target_link_libraries(base  GTest::gtest_main fmt::fmt tbb tbb xxhash antlr4-runtime)


add_executable(unit-test-base ${ALL_CPP_FILES})
# target_link_libraries(unit-test-base GTest::gtest_main fmt::fmt tbb tbb xxhash vql antlr4-runtime)
target_link_libraries(unit-test-base GTest::gtest_main fmt::fmt tbb tbb xxhash antlr4-runtime)
gtest_discover_tests(unit-test-base)

# function(add_custom_library target_name source_directory)
#     file(GLOB SOURCE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/${source_directory}/*.cpp")
#     add_library(${target_name} ${SOURCE_FILES})
#     target_link_libraries(${target_name} PUBLIC fmt::fmt spdlog::spdlog)
# endfunction()

# add_custom_library(meta "meta")
# add_custom_library(storage "storage")
# add_custom_library(execution "execution")
# add_custom_library(operator "operator")


add_executable(importer importer.cpp)
list(APPEND EXE_TARGETS importer)

target_link_libraries(importer base GTest::gtest GTest::gtest_main fmt::fmt tbb antlr4-runtime)


add_subdirectory(testify)
# add_subdirectory(vql)




