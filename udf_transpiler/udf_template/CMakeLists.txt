cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME udf1)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
project(${TARGET_NAME})
include_directories(src/include)


# file(GLOB UDF_SOURCES "src/*.cpp")
# set(UDF_SOURCES src/numeric.cpp src/cast.cpp)
set(EXTENSION_SOURCES src/udf1_extension.cpp)

# set(EXTENSION_SOURCES src/udf1_extension.cpp)
# add_library(udf_lib OBJECT ${UDF_SOURCES})
# target_include_directories(udf_lib PUBLIC src/include)
# set(UDF_OBJECT_FILES ${UDF_OBJECT_FILES} $<TARGET_OBJECTS:udf_lib>)
# add_library(${EXTENSION_NAME} STATIC src/udf1_extension.cpp ${UDF_OBJECT_FILES})
add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

# build_static_extension(${EXTENSION_NAME} ${EXTENSION_SOURCES})

# set(ALL_OBJECT_FILES
#     ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:${EXTENSION_NAME}>)

set(PARAMETERS "-warnings")
# build_loadable_extension(${TARGET_NAME} ${PARAMETERS} src/udf1_extension.cpp ${UDF_OBJECT_FILES})
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
