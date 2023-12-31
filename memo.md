1. need a command line parsing tool done
3. use the fmt formatting tool rather than std::format done
4. will need to have the modified binder run in the transpiler context instead of the generated code context
5. duckdb type to c++ type mapping



1. the function arg should not have name result or any name used in fshell2


in the binder, besides storing
1. sql name
1. function name
2. if template -> 
    1. if templated:
        1. input type options (logical)
        2. return type options ..
    2. else:
        1. input type (logical)
        2. return type ..
3. how to handle null values ->
    1. default handler i.e. input null return null
    2. input null return not guaranteed
4. whether it is a switch function ->
    1. it is: run the switch to and repeat
    2. not: generate the function
5. whether it is a string operator


<!-- should we modify the duckdb's binder or build ourselves'? -->
pros:
guaranteed same behavior
less code?

cons:
understand the whole thing
import the local variables to the catalog
reconstruct from the physical plan
must base on duckdb's source code


libpgquery macos arm or x86 switch

implicit cast on assignment


TODOs (2023.12.30):
1. case when assignment left hand side type mismatch with right hand side
Use the DuckDB **strict** casting rule to see if compatible, i.e.:
int --> long is compatible but decimal --> long is not
2. how to treat NULL?
have an isnull indicator for each variable
