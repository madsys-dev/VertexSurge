file(GLOB CPP_SOURCES *.cpp)

set(SPECIFIC_FILES_TO_REMOVE
    "${CMAKE_CURRENT_SOURCE_DIR}/landmark_on_the_fly.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/random_walk.cpp"
    # Add more files to remove as needed
)

file(GLOB PLAN_CPP_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/plans/*.cpp")
file(GLOB FIN_CPP_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/ldbc-finbench/*.cpp")

list(APPEND CPP_SOURCES ${PLAN_CPP_SOURCES})
list(APPEND CPP_SOURCES ${FIN_CPP_SOURCES})

# Remove specific files from the overall list
list(REMOVE_ITEM CPP_SOURCES ${SPECIFIC_FILES_TO_REMOVE})

# message(STATUS "CPP_SOURCES: ${CPP_SOURCES} ${SPECIFIC_FILES_TO_REMOVE}")

foreach(SOURCE ${CPP_SOURCES})
    get_filename_component(EXECUTABLE_NAME ${SOURCE} NAME_WE)
    add_executable(${EXECUTABLE_NAME} ${SOURCE})
    list(APPEND EXE_TARGETS ${EXECUTABLE_NAME})
    target_include_directories(${EXECUTABLE_NAME} PRIVATE xxHash_INCLUDE_DIR)
    target_link_libraries(${EXECUTABLE_NAME} base xxhash GTest::gtest GTest::gtest_main  tbb fmt::fmt )
endforeach()