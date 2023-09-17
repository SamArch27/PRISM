

/**
 * Scalar version of ScalarFunction::NopFunction
*/
template<typename A, typename ... Bs>
A return_first(A a, Bs... bs){
    return a;
}

