1. need a command line parsing tool done
2. need to modify the plpgsql parser done
3. use the fmt formatting tool rather than std::format done
4. will need to have the modified binder run in the transpiler context instead of the generated code context
5. duckdb type to c++ type mapping



1. the function arg should not have name result or any name used in fshell2


in the binder, besides storing
1. function name
2. if template -> 
    1. if templated:
        1. input type options (physical)
        2. return type options ..
    2. else:
        1. input type (physical)
        2. return type ..
3. how to handle null values ->
    1. default handler i.e. input null return null
    2. input null return not guaranteed