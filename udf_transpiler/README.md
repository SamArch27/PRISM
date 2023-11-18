# udf_transpiler

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

## Initialize
First time cloning the repository, also clone [this fork of duckdb](https://github.com/hkulyc/duckdb) to directory `duckdb`.

## 

## Profiler

```
make profile
```

In duckdb CLI (change path to yours specific)

```
install '/Users/lyc/Downloads/UDF-Transpiler/udf_transpiler/build/udfs/extension/profiler/profiler.duckdb_extension';
load '/Users/lyc/Downloads/UDF-Transpiler/udf_transpiler/build/udfs/extension/profiler/profiler.duckdb_extension';
pragma profile('/Users/lyc/Downloads/UDF-Transpiler/workload');
```

