//

//
#pragma once

#include <iostream>
#include <exception>
#include <thread>

#if defined(_WIN32)
#include <process.h>
#else
#include <unistd.h>
#endif
#include <iomanip>
#include <filesystem>
#include <set>      
#include <map>      
#include <string>
#include <vector>
#include <tuple>    
#include <chrono>   


#include "stax_db/db.h"
#include "stax_tx/transaction.h"
#include "stax_db/path_engine.h"
#include "stax_common/constants.h" 
#include "stax_db/query.h"
#include "stax_graph/graph_engine.h"
#include "benchmarks/throughput_bench.h"


#include "tests/common_test_utils.h" 
#include "tests/basic_correctness_tests.h"
#include "tests/compaction_tests.h"
#include "tests/init_test.h" 


namespace Tests { 











void run_all_tests() {
    run_basic_correctness_test();
    run_durability_test();
    run_concurrent_init_close_test(); 
   
    //run_hot_compaction_stress_test(); 
    //run_compaction_effectiveness_test(); 

    std::cout << "\nALL CORRECTNESS TESTS FINISHED SUCCESSFULLY!" << std::endl;
}

} 