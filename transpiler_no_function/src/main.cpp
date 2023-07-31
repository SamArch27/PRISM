#include <chrono>
#include <fstream>
#include <iostream>
#include <vector>
#include <string>
// #include <stdlib.h>
// #include <ctype.h>
#include <stdio.h>
// #include "duckdb.hpp"
#include <unistd.h>
#include "utils.hpp"
#include "trans.hpp"
#include <tclap/CmdLine.h>

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

    // Wrap everything in a try block.  Do this every time, 
	// because exceptions will be thrown for problems.
	try {  

	// Define the command line object, and insert a message
	// that describes the program. The "Command description message" 
	// is printed last in the help text. The second argument is the 
	// delimiter (usually space) and the last one is the version number. 
	// The CmdLine object parses the argv array based on the Arg objects
	// that it contains. 
	TCLAP::CmdLine cmd("Command description message", ' ', "0.1");

	// Define a value argument and add it to the command line.
	// A value arg defines a flag and a type of value that it expects,
	// such as "-n Bishop".
	TCLAP::ValueArg<std::string> dirArg("d","dir","Output to directory path (together with a test file)",false,"","string");
    TCLAP::ValueArg<std::string> fileArg("f","file","Output to file path",false,"","string");
	TCLAP::UnlabeledValueArg<std::string> inputArg("input", "Input file path", true, "", "nameString");
    cmd.add(inputArg);
    // Add the argument nameArg to the CmdLine object. The CmdLine object
	// uses this Arg to parse the command line.
	cmd.add(dirArg);
    cmd.add(fileArg);

	// Define a switch and add it to the command line.
	// A switch arg is a boolean argument and only defines a flag that
	// indicates true or false.  In this example the SwitchArg adds itself
	// to the CmdLine object as part of the constructor.  This eliminates
	// the need to call the cmd.add() method.  All args have support in
	// their constructors to add themselves directly to the CmdLine object.
	// It doesn't matter which idiom you choose, they accomplish the same thing.
	// TCLAP::SwitchArg reverseSwitch("r","reverse","Print name backwards", cmd, false);

	// Parse the argv array.
	cmd.parse(argc, argv);

	// Get the value parsed by each arg. 
	std::string o_dir = dirArg.getValue();
    std::string o_file = fileArg.getValue();
    std::string i_file = inputArg.getValue();
	// bool reverseName = reverseSwitch.getValue();
    // std::cout<<o_dir<<o_file<<i_file<<std::endl;

    std::ifstream t(i_file);
    std::ostringstream buffer;
    buffer << t.rdbuf();
    if(buffer.str().empty()){
        ERROR(fmt::format("Input file is empty or does not exist: {}", i_file));
    }
    YAMLConfig config;
    Catalog catalog;
    Catalog::Print(catalog);
    Transpiler transpiler(buffer.str(), &config, &catalog);
    // std::vector<std::string> ret = transpile_plpgsql_udf_str(buffer.str());
    std::vector<std::string> ret = transpiler.run();
    std::cout << ret[0] << std::endl;
    std::cout << ret[1] << std::endl;
    std::cout << ret[2] << std::endl;
    } 
    catch (TCLAP::ArgException &e){ 
        std::cerr << "error: " << e.error() << " for arg " << e.argId() << std::endl; 
    }
    
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