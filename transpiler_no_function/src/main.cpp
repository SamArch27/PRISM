#include <chrono>
#include <fstream>
#include <iostream>
#include <vector>
#include <string>
#include <stdlib.h>
#include <ctype.h>
#include <stdio.h>
// #include "duckdb.hpp"
#include <unistd.h>
#include "utils.hpp"
#include "trans.hpp"

using namespace std;

const string HELP_MES =
    "Allowed options:\n\
-h print this helper message;\n\
-f output to a file;\n\
-d output to a directory together with a test file;\n";
int add(int x, int y){
    return x+y;
}

int main(int argc, char const *argv[])
{
    // int aflag = 0;
    // int bflag = 0;
    // char *cvalue = NULL;
    char *fvalue = NULL;
    char *dvalue = NULL;
    int index;
    int c;

    opterr = 0;

    while ((c = getopt(argc, (char *const *)argv, "hf:d:")) != -1){
        switch (c)
        {
        case 'h':
            std::cout << HELP_MES << std::endl;
            break;
        case 'f':
            // bflag = 1;
            fvalue = optarg;
            std::cout << fvalue << std::endl;
            break;
        case 'd':
            // cvalue = optarg;
            dvalue = optarg;
            std::cout << dvalue << std::endl;
            break;
        default:
            abort();
        }
    }
    const char *input_file = NULL;
    for (index = optind; index < argc; index++)
        if(input_file == NULL){
            input_file = argv[index];
        }
        else{
            throw std::runtime_error("More than one input file is provided.");
        }
        // printf ("Non-option argument %s\n", argv[index]);
    std::ifstream t(input_file);
    std::ostringstream buffer;
    buffer << t.rdbuf();
    if(buffer.str().empty()){
        throw std::runtime_error(fmt::format("Input file is empty or does not exist: {}", input_file));
    }
    std::vector<std::string> ret = transpile_plpgsql_udf_str(buffer.str());
    std::cout << ret[0] << std::endl;
    std::cout << ret[1] << std::endl;
    std::cout << ret[2] << std::endl;
    
    return 0;
}






// printf("aflag = %d, bflag = %d, cvalue = %s\n",
    //        aflag, bflag, cvalue);
    
    // int i = utils::unpack_caller(std::max, {0,1});
    // struct format_caller<3> fc;
    // std::vector<std::string> input({"ads", "dsa", "ads"});
    // std::string i = fc(fmt::format, "{} {} {}", input);
    // std::cout<<i<<std::endl;
    // std::cout<<fmt::format("{}{}", "ad", "ads")<<std::endl;