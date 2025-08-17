

#include <iostream>
#include <exception>
#include <thread> 
#include <chrono> 
#include <future> 
#include <csignal>
#include <unistd.h>


#include "benchmarks/benchmarks.h" 
#include "tests/tests.h" 
#include "benchmarks/tcp_bench.h" 


#define TCP_SERVER_PORT 13371

void timeout_handler(int signum) {
    std::cerr << "\n\n******************************************************************" << std::endl;
    std::cerr << "*** BENCHMARK TIMEOUT: Exceeded 80 seconds. Aborting. ***" << std::endl;
    std::cerr << "******************************************************************\n" << std::endl;
    _exit(143); // Exit code 143 for timeout
}


int main(int argc, char* argv[]) {
    try {
        
        Tests::run_all_tests();

        signal(SIGALRM, timeout_handler);
        alarm(80); // Set a 20-second alarm
        
        run_all_benchmarks();

        alarm(0); // Disable the alarm
 
        std::cout << "\nAll tests and benchmarks finished successfully." << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "\nFATAL ERROR in main: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}