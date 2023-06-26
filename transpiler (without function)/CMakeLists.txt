cmake_minimum_required(VERSION 3.17)
project(transpiler)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "-O2 -Wall")


include_directories(${CMAKE_SOURCE_DIR}/dependencies/libduckdb-osx-universal) # *.h
link_directories(${CMAKE_SOURCE_DIR}/dependencies/libduckdb-osx-universal)    # *.dylib

# include_directories(${CMAKE_SOURCE_DIR}/dependencies/libduckdb-src) # *.h
# link_directories(${CMAKE_SOURCE_DIR}/dependencies/libduckdb-src)    # *.dylib

add_executable(main main.cpp)

target_link_libraries(main duckdb)