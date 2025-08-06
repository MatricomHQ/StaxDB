#pragma once

#include <cstddef>

/**
 * @brief Runs the TPC-C benchmark against our database.
 * 
 * This function initializes the database with a specified number of warehouses,
 * then simulates a number of concurrent users (terminals) executing transactions
 * against the database for a fixed duration. It measures and reports the
 * primary TPC-C metric: new-order transactions per minute (tpmC).
 * 
 * @param num_warehouses The number of warehouses to populate in the database. This
 *                       is the primary scaling factor for the TPC-C workload.
 * @param num_threads The number of concurrent threads to run, each simulating a
 *                    user terminal executing transactions.
 */
void run_tpcc_benchmark(size_t num_warehouses, size_t num_threads);