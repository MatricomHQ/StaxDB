
#include "tpcc.h"
#include "stax_db/db.h"
#include "stax_common/constants.h"
#include "stax_tx/transaction.h" 
#include "tests/common_test_utils.h" 

#include <iostream>
#include <vector>
#include <thread>
#include <memory>
#include <stdexcept>
#include <random>
#include <chrono>
#include <numeric>
#include <algorithm>
#include <atomic>
#include <string>
#include <iomanip>
#include <cstring> 
#include <filesystem> 


#if defined(_WIN32)
#include <process.h>
#else
#include <unistd.h> 
#endif





namespace TPCC {

const int NUM_ITEMS = 100000;
const int DISTRICTS_PER_WAREHOUSE = 10;
const int CUSTOMERS_PER_DISTRICT = 3000;
const int ORDERS_PER_DISTRICT = 3000;


std::string warehouse_key(int32_t w_id, const std::string& col) { return "w/" + std::to_string(w_id) + ":" + col; }
std::string item_key(int32_t i_id, const std::string& col) { return "i/" + std::to_string(i_id) + ":" + col; }
std::string stock_key(int32_t w_id, int32_t i_id, const std::string& col) { return "s/" + std::to_string(w_id) + "/" + std::to_string(i_id) + ":" + col; }
std::string district_key(int32_t w_id, int32_t d_id, const std::string& col) { return "d/" + std::to_string(w_id) + "/" + std::to_string(d_id) + ":" + col; }
std::string customer_key(int32_t w_id, int32_t d_id, int32_t c_id, const std::string& col) { return "c/" + std::to_string(w_id) + "/" + std::to_string(d_id) + "/" + std::to_string(c_id) + ":" + col; }
std::string order_key(int32_t w_id, int32_t d_id, int32_t o_id, const std::string& col) { return "o/" + std::to_string(w_id) + "/" + std::to_string(d_id) + "/" + std::to_string(o_id) + ":" + col; }
std::string order_line_key(int32_t w_id, int32_t d_id, int32_t o_id, int32_t ol_num, const std::string& col) { return "ol/" + std::to_string(w_id) + "/" + std::to_string(d_id) + "/" + std::to_string(o_id) + "/" + std::to_string(ol_num) + ":" + col; }
std::string new_order_key(int32_t w_id, int32_t d_id, int32_t o_id) { return "no/" + std::to_string(w_id) + "/" + std::to_string(d_id) + "/" + std::to_string(o_id); }
std::string district_prefix(int32_t w_id, int32_t d_id) { return "d/" + std::to_string(w_id) + "/" + std::to_string(d_id) + ":"; }

class TPCCRandom {
private:
    std::mt19937 gen;
    int C_last, C_id, ol_i_id;
public:
    TPCCRandom() {
        std::random_device rd;
        gen.seed(rd());
        C_last = uniform_int(0, 255); C_id = uniform_int(0, 1023); ol_i_id = uniform_int(0, 8191);
    }
    int uniform_int(int min, int max) { return std::uniform_int_distribution<>(min, max)(gen); }
    int non_uniform_rand(int A, int x, int y) {
        int C = 0;
        if (A == 255) C = C_last; else if (A == 1023) C = C_id; else if (A == 8191) C = ol_i_id;
        return (((uniform_int(0, A) | uniform_int(x, y)) + C) % (y - x + 1)) + x;
    }
    std::string rand_astring(int min_len, int max_len) {
        int len = uniform_int(min_len, max_len);
        std::string s(len, ' ');
        for(int i = 0; i < len; ++i) s[i] = (char)uniform_int('a', 'z');
        return s;
    }
};

class TPCCLoader {
public:
    
    TPCCLoader(Database* db, uint32_t collection_idx, size_t num_warehouses) 
        : db_(db), collection_idx_(collection_idx), num_warehouses_(num_warehouses) {}

    void load_data() {
        std::cout << "TPC-C Loader: Populating StaxDB with " << num_warehouses_ << " warehouse(s) (Columnar)...." << std::endl;
        TPCCRandom random;
        

        
        Collection& col = db_->get_collection_by_idx(collection_idx_);
        
        
        TxnContext item_ctx = col.begin_transaction_context(0, false); 
        TransactionBatch item_batch;
        
        for(int i = 1; i <= NUM_ITEMS; ++i) {
            col.insert(item_ctx, item_batch, item_key(i, "id"), std::to_string(i));
            col.insert(item_ctx, item_batch, item_key(i, "im_id"), std::to_string(random.uniform_int(1, 10000)));
            col.insert(item_ctx, item_batch, item_key(i, "name"), random.rand_astring(14, 24));
            col.insert(item_ctx, item_batch, item_key(i, "price"), std::to_string(random.uniform_int(100, 10000) / 100.0));
        }
        col.commit(item_ctx, item_batch);
        std::cout << "  - Items loaded." << std::endl;

        for(int w_id = 1; w_id <= num_warehouses_; ++w_id) {
            Collection& wh_col = db_->get_collection_by_idx(collection_idx_); 
            TxnContext wh_ctx = wh_col.begin_transaction_context(w_id % BENCHMARK_NUM_THREADS, false); 
            TransactionBatch wh_batch;

            wh_col.insert(wh_ctx, wh_batch, warehouse_key(w_id, "id"), std::to_string(w_id));
            
            for(int i_id = 1; i_id <= NUM_ITEMS; ++i_id) {
                wh_col.insert(wh_ctx, wh_batch, stock_key(w_id, i_id, "w_id"), std::to_string(w_id));
                wh_col.insert(wh_ctx, wh_batch, stock_key(w_id, i_id, "i_id"), std::to_string(i_id));
                wh_col.insert(wh_ctx, wh_batch, stock_key(w_id, i_id, "quantity"), std::to_string(random.uniform_int(10, 100)));
            }

            for(int d_id = 1; d_id <= DISTRICTS_PER_WAREHOUSE; ++d_id) {
                wh_col.insert(wh_ctx, wh_batch, district_key(w_id, d_id, "id"), std::to_string(d_id));
                wh_col.insert(wh_ctx, wh_batch, district_key(w_id, d_id, "w_id"), std::to_string(w_id));
                wh_col.insert(wh_ctx, wh_batch, district_key(w_id, d_id, "next_o_id"), std::to_string(ORDERS_PER_DISTRICT + 1));
            }
            wh_col.commit(wh_ctx, wh_batch);
            std::cout << "  - Warehouse " << w_id << " loaded." << std::endl;
        }
        std::cout << "TPC-C Loader: Data loading complete." << std::endl;
    }
private:
    Database* db_;
    uint32_t collection_idx_;
    size_t num_warehouses_;
};

class TPCCTerminal {
    Database* db_;
    uint32_t collection_idx_;
    size_t num_warehouses_;
    size_t thread_id_;
    TPCCRandom random_;
    std::atomic<long long>& new_order_count_;

public:
    TPCCTerminal(Database* db, uint32_t collection_idx, size_t num_warehouses, size_t thread_id, std::atomic<long long>& new_order_count)
        : db_(db), collection_idx_(collection_idx), num_warehouses_(num_warehouses), thread_id_(thread_id), new_order_count_(new_order_count) {
        }

    void execute_new_order() {
        
        Collection& col = db_->get_collection_by_idx(collection_idx_);
        TxnContext ctx = col.begin_transaction_context(thread_id_, false); 
        TransactionBatch batch;
        
        int32_t w_id = random_.uniform_int(1, num_warehouses_);
        int32_t d_id = random_.uniform_int(1, DISTRICTS_PER_WAREHOUSE);
        int32_t c_id = random_.non_uniform_rand(1023, 1, CUSTOMERS_PER_DISTRICT);
        int32_t ol_cnt = random_.uniform_int(5, 15);

        auto next_o_id_opt = col.get(ctx, district_key(w_id, d_id, "next_o_id"));
        if(!next_o_id_opt) { col.abort(ctx); return; } 
        int32_t o_id = std::stoi(std::string(next_o_id_opt->value_view()));

        col.insert(ctx, batch, district_key(w_id, d_id, "next_o_id"), std::to_string(o_id + 1));
        col.insert(ctx, batch, order_key(w_id, d_id, o_id, "id"), std::to_string(o_id));
        col.insert(ctx, batch, order_key(w_id, d_id, o_id, "c_id"), std::to_string(c_id));
        col.insert(ctx, batch, order_key(w_id, d_id, o_id, "ol_cnt"), std::to_string(ol_cnt));
        col.insert(ctx, batch, new_order_key(w_id, d_id, o_id), "1"); 
        
        for(int i = 1; i <= ol_cnt; ++i) {
            int32_t i_id = random_.non_uniform_rand(8191, 1, NUM_ITEMS);
            auto item_price_opt = col.get(ctx, item_key(i_id, "price"));
            if(!item_price_opt) { col.abort(ctx); return; } 

            auto stock_quantity_opt = col.get(ctx, stock_key(w_id, i_id, "quantity"));
            if(!stock_quantity_opt) { col.abort(ctx); return; } 
            int32_t s_quantity = std::stoi(std::string(stock_quantity_opt->value_view()));

            
            if(s_quantity > 10) s_quantity += random_.uniform_int(1, 10); 
            else s_quantity += 91 + random_.uniform_int(1, 10); 
            
            col.insert(ctx, batch, stock_key(w_id, i_id, "quantity"), std::to_string(s_quantity));
            col.insert(ctx, batch, order_line_key(w_id, d_id, o_id, i, "i_id"), std::to_string(i_id));
        }
        
        col.commit(ctx, batch);
        new_order_count_++;
    }
    
    void run_for(std::chrono::seconds duration, const std::atomic<bool>& stop_flag) {
        while(!stop_flag.load()) {
            execute_new_order();
        }
    }
};

} 











void run_tpcc_benchmark(size_t num_warehouses, size_t num_threads) {
    std::cout << "\n==========================================================================================" << std::endl;
    std::cout << "--- TPC-C BENCHMARK SUITE (VECTORIZED COLUMNAR) ---" << std::endl;
    std::cout << "==========================================================================================" << std::endl;
    std::cout << "Configuration: " << num_warehouses << " Warehouse(s), " << num_threads << " Thread(s)" << std::endl;
    if (num_warehouses == 0 || num_threads == 0) {
        throw std::invalid_argument("TPC-C: Number of warehouses and threads must be greater than zero.");
    }

    std::filesystem::path db_base_dir = "./db_data";
    std::filesystem::path db_dir = db_base_dir / ("tpcc_db_" + std::to_string(Tests::get_process_id()));
    
    
    if (std::filesystem::exists(db_base_dir)) {
        try {
            std::filesystem::remove_all(db_base_dir);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Warning: Could not remove directory " << db_base_dir << ": " << e.what() << std::endl;
        }
    }
    std::filesystem::create_directories(db_dir);

    auto db = Database::create_new(db_dir, num_threads);
    
    uint32_t tpcc_collection_idx = db->get_collection("tpcc");

    TPCC::TPCCLoader loader(db.get(), tpcc_collection_idx, num_warehouses);
    loader.load_data();
    
    std::cout << "TPC-C: Starting workload..." << std::endl;
    std::atomic<bool> stop_flag(false);
    std::atomic<long long> total_new_orders(0);
    std::vector<std::thread> terminals;

    auto benchmark_start_time = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < num_threads; ++i) {
        terminals.emplace_back([&, i]() {
            TPCC::TPCCTerminal terminal(db.get(), tpcc_collection_idx, num_warehouses, i, total_new_orders);
            terminal.run_for(std::chrono::seconds(10), stop_flag);
        });
    }

    std::this_thread::sleep_for(std::chrono::seconds(10)); 
    stop_flag.store(true); 
    for (auto& t : terminals) {
        if (t.joinable()) t.join(); 
    }

    auto benchmark_end_time = std::chrono::high_resolution_clock::now();
    auto duration_s = std::chrono::duration<double>(benchmark_end_time - benchmark_start_time).count();
    double tpmc = (total_new_orders.load() * 60.0) / duration_s;

    std::cout << "\n--- TPC-C Results (StaxDB - Vectorized Columnar) ---" << std::endl;
    std::cout << "Workload duration: " << std::fixed << std::setprecision(2) << duration_s << " seconds" << std::endl;
    std::cout << "Total New-Order transactions: " << total_new_orders.load() << std::endl;
    std::cout << "tpmC (New-Order Transactions Per Minute): " << std::fixed << std::setprecision(2) << tpmc << std::endl;

    
    if (std::filesystem::exists(db_base_dir)) {
        try {
            std::filesystem::remove_all(db_base_dir);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Warning: Could not remove directory " << db_base_dir << " after benchmark: " << e.what() << std::endl;
        }
    }
}