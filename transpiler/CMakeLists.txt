# cmake_minimum_required(VERSION 2.8.12)

# # Set extension name here
# set(TARGET_NAME udf_transpiler)

# set(EXTENSION_NAME ${TARGET_NAME}_extension)
# project(${TARGET_NAME})
# include_directories(src/include)


# # file(GLOB UDF_SOURCES "src/*.cpp")
# # set(UDF_SOURCES src/numeric.cpp src/cast.cpp)
# set(EXTENSION_SOURCES src/udf_transpiler_extension.cpp)

# # set(EXTENSION_SOURCES src/udf_transpiler_extension.cpp)
# # add_library(udf_lib OBJECT ${UDF_SOURCES})
# # target_include_directories(udf_lib PUBLIC src/include)
# # set(UDF_OBJECT_FILES ${UDF_OBJECT_FILES} $<TARGET_OBJECTS:udf_lib>)
# # add_library(${EXTENSION_NAME} STATIC src/udf_transpiler_extension.cpp ${UDF_OBJECT_FILES})
# add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

# # build_static_extension(${EXTENSION_NAME} ${EXTENSION_SOURCES})

# # set(ALL_OBJECT_FILES
# #     ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:${EXTENSION_NAME}>)

# set(PARAMETERS "-warnings")
# # build_loadable_extension(${TARGET_NAME} ${PARAMETERS} src/udf_transpiler_extension.cpp ${UDF_OBJECT_FILES})
# build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

# install(
#   TARGETS ${EXTENSION_NAME}
#   EXPORT "${DUCKDB_EXPORT_SET}"
#   LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
#   ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

cmake_minimum_required(VERSION 3.24.2)
project(transpiler)

set(CMAKE_CXX_STANDARD 20)
# set(CMAKE_CXX_COMPILER "clang++")
set(CMAKE_CXX_FLAGS "-O2 -Wall -pthread")
# set(CMAKE_OSX_ARCHITECTURES "x86_64" CACHE INTERNAL "" FORCE)

# duckdb lib
# include_directories(${CMAKE_SOURCE_DIR}/src/dependencies/libduckdb-osx-universal) # *.h
# link_directories(${CMAKE_SOURCE_DIR}/src/dependencies/libduckdb-osx-universal)    # *.dylib

# the commandline lib
include_directories(${CMAKE_SOURCE_DIR}/src/dependencies/tclap) # *.h
# include_directories(${CMAKE_SOURCE_DIR}/src/dependencies/tclap)

# plpgsql parser lib
set(PG_PARSER_ROOT ${CMAKE_SOURCE_DIR}/src/dependencies/libpg_query)
include_directories(${PG_PARSER_ROOT})
add_custom_target(pg_parser
    COMMAND make build
    WORKING_DIRECTORY ${PG_PARSER_ROOT}
)

# the formatting lib
add_subdirectory(${CMAKE_SOURCE_DIR}/src/dependencies/fmt)
include_directories(${CMAKE_SOURCE_DIR}/src/dependencies/fmt/include)

# the parser lib
add_subdirectory(${CMAKE_SOURCE_DIR}/src/dependencies/yaml-cpp)
include_directories(${CMAKE_SOURCE_DIR}/src/dependencies/yaml-cpp/include)

# the json formatter lib
include_directories(${CMAKE_SOURCE_DIR}/src/dependencies/json)

# boost lib
# # set(BOOST_ROOT ${CMAKE_SOURCE_DIR}/src/dependencies/boost_1_82_0/)
# set(Boost_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/src/dependencies/boost_1_82_0/)
# set(Boost_LIBRARY_DIR ${Boost_INCLUDE_DIR}/libs)
# # program options
# # add_subdirectory(${Boost_LIBRARY_DIR}/program_options)
# # find_package(Boost REQUIRED COMPONENTS program_options)
# include_directories(${Boost_INCLUDE_DIR})
# link_directories(${Boost_LIBRARY_DIR})
# # include_directories(${Boost_INCLUDE_DIR})
# # include_directories(${CMAKE_SOURCE_DIR}/src/dependencies/libduckdb-src) # *.h
# # link_directories(${CMAKE_SOURCE_DIR}/src/dependencies/libduckdb-src)    # *.dylib

# include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_SOURCE_DIR}/src/include)
include_directories(${CMAKE_SOURCE_DIR}/duckdb/src/include)
file(GLOB all_SRCS
        # "${PROJECT_SOURCE_DIR}/include/*.h"
        # "${PROJECT_SOURCE_DIR}/include/*.hpp"
        "${PROJECT_SOURCE_DIR}/src/*.cpp"
        "${PROJECT_SOURCE_DIR}/src/*.c"
    )
add_executable(main ${all_SRCS})
add_dependencies(main pg_parser)

add_subdirectory(duckdb)
add_dependencies(main duckdb)
target_link_libraries(main ${PG_PARSER_ROOT}/libpg_query.a fmt yaml-cpp duckdb)
