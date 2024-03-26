.PHONY: all clean format debug release duckdb_debug duckdb_release pull update

all: release

MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJ_DIR := $(dir $(MKFILE_PATH))

OSX_BUILD_UNIVERSAL_FLAG=
ifeq (${OSX_BUILD_UNIVERSAL}, 1)
	OSX_BUILD_UNIVERSAL_FLAG=-DOSX_BUILD_UNIVERSAL=1
endif
ifeq (${STATIC_LIBCPP}, 1)
	STATIC_LIBCPP=-DSTATIC_LIBCPP=TRUE
endif

ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
	FORCE_COLOR=-DFORCE_COLORED_OUTPUT=1
endif

BUILD_FLAGS=-DEXTENSION_STATIC_BUILD=1 -DBUILD_TPCH_EXTENSION=1 -DBUILD_TPCDS_EXTENSION=1 -DBUILD_PARQUET_EXTENSION=1 -DBUILD_EXTENSIONS='tpch;tpcds' ${OSX_BUILD_UNIVERSAL_FLAG} ${STATIC_LIBCPP}

CLIENT_FLAGS :=

# These flags will make DuckDB build the extension
EXTENSION_FLAGS=-DDUCKDB_EXTENSION_NAMES="udf_transpiler" -DDUCKDB_EXTENSION_UDF_TRANSPILER_PATH="$(PROJ_DIR)" -DDUCKDB_EXTENSION_UDF_TRANSPILER_SHOULD_LINK="TRUE" -DDUCKDB_EXTENSION_UDF_TRANSPILER_INCLUDE_PATH="$(PROJ_DIR)src/include"

pull:
	git submodule init
	git submodule update --recursive --remote

clean:
	rm -rf build
	rm -rf testext
	rm -rf udf1
	cd duckdb && make clean

# Main build
debug:
	mkdir -p  build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Debug ${BUILD_FLAGS} -S ./duckdb/ -B build/debug && \
	cmake --build build/debug --config Debug -j4

UDF_UDF1_EXTENSION_FLAGS=-DBUILD_MAIN_DUCKDB_LIBRARY=0 -DDISABLE_BUILTIN_EXTENSIONS=1 -DDUCKDB_EXTENSION_NAMES="udf1" -DDUCKDB_EXTENSION_UDF1_PATH="$(PROJ_DIR)udf1" -DDUCKDB_EXTENSION_UDF1_INCLUDE_PATH="$(PROJ_DIR)udf1/src/include"
udfs:
	mkdir -p build/udfs
	mkdir -p udf1
	- cp -r -n udf_template/* udf1
	cmake $(GENERATOR) $(FORCE_COLOR) $(UDF_UDF1_EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=0 -DCMAKE_BUILD_TYPE=Release -S ./duckdb/ -B build/udfs
	cmake --build build/udfs --config Release

UDF_UDF_AGG_EXTENSION_FLAGS=-DBUILD_MAIN_DUCKDB_LIBRARY=0 -DDISABLE_BUILTIN_EXTENSIONS=1 -DDUCKDB_EXTENSION_NAMES="udf_agg" -DDUCKDB_EXTENSION_UDF_AGG_PATH="$(PROJ_DIR)udf_agg" -DDUCKDB_EXTENSION_UDF_AGG_INCLUDE_PATH="$(PROJ_DIR)udf_agg/src/include"
udafs:
	mkdir -p build/udfs
	cmake $(GENERATOR) $(FORCE_COLOR) $(UDF_UDF_AGG_EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=0 -DCMAKE_BUILD_TYPE=Release -S ./duckdb/ -B build/udfs
	cmake --build build/udfs --config Release

PROFILER_EXTENSION_FLAGS=-DBUILD_MAIN_DUCKDB_LIBRARY=0 -DDISABLE_BUILTIN_EXTENSIONS=1 -DDUCKDB_EXTENSION_NAMES="profiler" -DDUCKDB_EXTENSION_PROFILER_PATH="$(PROJ_DIR)profiler" -DDUCKDB_EXTENSION_PROFILER_INCLUDE_PATH="$(PROJ_DIR)profiler/src/include"
profile:
	mkdir -p build/release
	cmake $(GENERATOR) $(FORCE_COLOR) $(PROFILER_EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=0 -DCMAKE_BUILD_TYPE=Release -S ./duckdb/ -B build/release
	cmake --build build/release --config Release

release:
	- make udfs
	mkdir -p build/release
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Release ${BUILD_FLAGS} -S ./duckdb/ -B build/release && \
	cmake --build build/release --config Release -j10

start_profile:
	./build/release/duckdb -unsigned -cmd "install './build/release/extension/profiler/profiler.duckdb_extension'; load './build/release/extension/profiler/profiler.duckdb_extension';"

# Client build
debug_js: CLIENT_FLAGS=-DBUILD_NODE=1 -DBUILD_JSON_EXTENSION=1
debug_js: debug

debug_r: CLIENT_FLAGS=-DBUILD_R=1
debug_r: debug

debug_python: CLIENT_FLAGS=-DBUILD_PYTHON=1 -DBUILD_JSON_EXTENSION=1 -DBUILD_FTS_EXTENSION=1 -DBUILD_TPCH_EXTENSION=1 -DBUILD_VISUALIZER_EXTENSION=1 -DBUILD_TPCDS_EXTENSION=1
debug_python: debug

release_js: CLIENT_FLAGS=-DBUILD_NODE=1 -DBUILD_JSON_EXTENSION=1
release_js: release

release_r: CLIENT_FLAGS=-DBUILD_R=1
release_r: release

release_python: CLIENT_FLAGS=-DBUILD_PYTHON=1 -DBUILD_JSON_EXTENSION=1 -DBUILD_FTS_EXTENSION=1 -DBUILD_TPCH_EXTENSION=1 -DBUILD_VISUALIZER_EXTENSION=1 -DBUILD_TPCDS_EXTENSION=1
release_python: release

# Main tests
test: test_release

test_release: release
	./build/release/test/unittest --test-dir . "[sql]"

test_debug: debug
	./build/debug/test/unittest --test-dir . "[sql]"

# Client tests
test_js: test_debug_js
test_debug_js: debug_js
	cd duckdb/tools/nodejs && npm run test-path -- "../../../test/nodejs/**/*.js"

test_release_js: release_js
	cd duckdb/tools/nodejs && npm run test-path -- "../../../test/nodejs/**/*.js"

test_python: test_debug_python
test_debug_python: debug_python
	cd test/python && python3 -m pytest

test_release_python: release_python
	cd test/python && python3 -m pytest

format:
	find src/ -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
	cmake-format -i CMakeLists.txt

update:
	git submodule update --remote --merge

run:
	./build/release/duckdb -unsigned
	


