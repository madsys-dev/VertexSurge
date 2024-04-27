

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/grammar/vqlLexer.cpp ${CMAKE_CURRENT_SOURCE_DIR}/grammar/vqlParser.cpp ${CMAKE_CURRENT_SOURCE_DIR}/grammar/vqlBaseListener.cpp ${CMAKE_CURRENT_SOURCE_DIR}/grammar/vqlListener.cpp
    COMMAND antlr4 -Dlanguage=Cpp ${CMAKE_CURRENT_SOURCE_DIR}/grammar/vql.g4
    DEPENDS grammar/vql.g4
    COMMENT "Running antlr4 cpp generation"
)
SET(VQL_FILES query_builder.cpp grammar/vqlLexer.cpp grammar/vqlParser.cpp grammar/vqlBaseListener.cpp grammar/vqlListener.cpp)

add_library(vql STATIC ${VQL_FILES})

add_executable(vql-interface vql-interface.cpp)
target_link_libraries(vql-interface base vql xxhash GTest::gtest_main tbb fmt::fmt antlr4-runtime)

