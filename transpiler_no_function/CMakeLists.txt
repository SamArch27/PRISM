cmake_minimum_required(VERSION 3.24.2)
project(transpiler)

set(CMAKE_CXX_STANDARD 20)
# set(CMAKE_CXX_COMPILER "clang++")
set(CMAKE_CXX_FLAGS "-O2 -Wall -pthread")
# set(CMAKE_OSX_ARCHITECTURES "x86_64" CACHE INTERNAL "" FORCE)

# duckdb lib
include_directories(${CMAKE_SOURCE_DIR}/dependencies/libduckdb-osx-universal) # *.h
link_directories(${CMAKE_SOURCE_DIR}/dependencies/libduckdb-osx-universal)    # *.dylib

# the commandline lib
include_directories(${CMAKE_SOURCE_DIR}/dependencies/tclap) # *.h
# include_directories(${CMAKE_SOURCE_DIR}/dependencies/tclap)

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

# the parser lib
add_subdirectory(${CMAKE_SOURCE_DIR}/dependencies/yaml-cpp)
include_directories(${CMAKE_SOURCE_DIR}/dependencies/yaml-cpp/include)

# the json formatter lib
include_directories(${CMAKE_SOURCE_DIR}/dependencies/json)

# the pretty print lib
include_directories(${CMAKE_SOURCE_DIR}/dependencies/pretty-print)
# add_subdirectory(${CMAKE_SOURCE_DIR}/dependencies/pretty-print)

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

include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_SOURCE_DIR}/include)
file(GLOB all_SRCS
        # "${PROJECT_SOURCE_DIR}/include/*.h"
        # "${PROJECT_SOURCE_DIR}/include/*.hpp"
        "${PROJECT_SOURCE_DIR}/src/*.cpp"
        "${PROJECT_SOURCE_DIR}/src/*.c"
    )
add_executable(main ${all_SRCS})
add_dependencies(main pg_parser)

target_link_libraries(main ${PG_PARSER_ROOT}/libpg_query.a fmt yaml-cpp)
