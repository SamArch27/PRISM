# PRISM

## Run

1. `make`
   This will first build DuckDB, then it will build PRISM as an extension to DuckDB.

2. `make run`
   This will start the DuckDB shell and it will load the prism extension (called udf_transpiler) automatically.

3. Copy in this UDF to the shell.
   ```
   CREATE FUNCTION addAbs(val1 INT, val2 INT) RETURNS INT AS $$
   BEGIN
   IF val1 < 0 THEN
   val1 = -val1;
   END IF;
   IF val2 < 0 THEN
   val2 = -val2;
   END IF;
   RETURN val1 + val2;
   END; $$
   LANGUAGE PLPGSQL;
   ```

4. You should be able to see the `Transpilation Done.` message.

5. Create a macro to wrap the outlined udf.
   ```
   create macro addAbs(val1, val2) as (addAbs_outlined_0(val1, val2));
   ```

6. `select addAbs(-2, -3);`
7. The result should be `5`.

## Test

This will run some test cases to make sure the prism is working fine.
```
make test
```

## Profiler

```
make profile
```

In duckdb CLI (change path to yours specific)

```
install './build/udfs/extension/profiler/profiler.duckdb_extension';
load './build/udfs/extension/profiler/profiler.duckdb_extension';
pragma profile('./workload');
```

