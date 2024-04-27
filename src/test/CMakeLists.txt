

add_executable(unit-test gtest.cpp)
target_link_libraries(unit-test base GTest::gtest fmt::fmt tbb)

gtest_discover_tests(unit-test)
