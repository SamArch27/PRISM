cmake_minimum_required(VERSION 3.24.2)
project(transpiler)

set(CMAKE_CXX_STANDARD 20)
# set(CMAKE_CXX_COMPILER "clang++")
set(CMAKE_CXX_FLAGS "-O2 -Wall")
# set(CMAKE_OSX_ARCHITECTURES "x86_64" CACHE INTERNAL "" FORCE)

# duckdb lib
include_directories(${CMAKE_SOURCE_DIR}/dependencies/libduckdb-osx-universal) # *.h
link_directories(${CMAKE_SOURCE_DIR}/dependencies/libduckdb-osx-universal)    # *.dylib

# plpgsql parser lib
set(PG_PARSER_ROOT ${CMAKE_SOURCE_DIR}/dependencies/libpg_query)
include_directories(${PG_PARSER_ROOT})
add_custom_target(pg_parser
    COMMAND make build
    WORKING_DIRECTORY ${PG_PARSER_ROOT}
)

# the formatting lib
add_subdirectory(${CMAKE_SOURCE_DIR}/dependencies/fmt)
include_directories(${CMAKE_SOURCE_DIR}/dependencies/fmt/include)

# boost lib
# # set(BOOST_ROOT ${CMAKE_SOURCE_DIR}/dependencies/boost_1_82_0/)
# set(Boost_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/dependencies/boost_1_82_0/)
# set(Boost_LIBRARY_DIR ${Boost_INCLUDE_DIR}/libs)
# # program options
# # add_subdirectory(${Boost_LIBRARY_DIR}/program_options)
# # find_package(Boost REQUIRED COMPONENTS program_options)
# include_directories(${Boost_INCLUDE_DIR})
# link_directories(${Boost_LIBRARY_DIR})
# # include_directories(${Boost_INCLUDE_DIR})
# # include_directories(${CMAKE_SOURCE_DIR}/dependencies/libduckdb-src) # *.h
# # link_directories(${CMAKE_SOURCE_DIR}/dependencies/libduckdb-src)    # *.dylib

add_executable(main main.cpp)
add_dependencies(main pg_parser)

target_link_libraries(main duckdb ${PG_PARSER_ROOT}/libpg_query.a fmt)