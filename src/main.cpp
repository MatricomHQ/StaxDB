

#include <iostream>
#include <exception>
#include <thread> 
#include <chrono> 
#include <future> 


#include "benchmarks/benchmarks.h" 
#include "tests/tests.h" 
#include "benchmarks/tcp_bench.h" 


#define TCP_SERVER_PORT 13371

int main(int argc, char* argv[]) {
    try {
        
        Tests::run_all_tests();



        
        run_all_benchmarks();
 
        std::cout << "\nAll tests and benchmarks finished successfully." << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "\nFATAL ERROR in main: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}