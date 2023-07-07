# UDF-Transpiler
Transpile PL/pgSQL UDFs to C++ in DuckDB

## Loop

Supports `LOOP`, `EXIT`, `CONTINUE`, `WHILE`, `FOR`, with limitations of:
- Does not support specifying label. For instance, `EXIT [ label ]` cannot be called to exit from the loop labeled with `label`. The same holds true for `CONTINUE` as well.
- Does not support `WHEN` for  `EXIT` and `CONTINUE`. One should use explicit conditional statement instead.

In for loop, the iterated variable (i.e.`name` in `FOR name IN [ REVERSE ] expression .. expression [ BY expression ] LOOP`) is handled by:
- Creating a fresh variable `tempvar` specificaly within the loop's scope
- All use of `name` in the loop scope will be replaced by `tempvar`
- If nested loops have the same iterated variable, a new `tempvar2` will be created and used in the inner loop