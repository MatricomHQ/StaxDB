cmake_minimum_required(VERSION 3.10)

project(StaxDB C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# For Windows shared library builds, this automatically handles exporting
# symbols, which is required for other targets to link against the generated
# .dlls via their .lib import libraries. This resolves LNK1181 errors.
if(WIN32)
  set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS TRUE)
endif()

# Define build types
set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build." FORCE)
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "RelWithDebInfo" "MinSizeRel")

# Set optimization flags conditionally. MSVC will use its defaults for Release/Debug.
if(NOT MSVC)
    set(CMAKE_C_FLAGS_RELEASE "-O3 -DNDEBUG" CACHE STRING "Flags used by the C compiler for release builds." FORCE)
    set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG" CACHE STRING "Flags used by the CXX compiler for release builds." FORCE)
    set(CMAKE_C_FLAGS_DEBUG "-g -O0" CACHE STRING "Flags used by the C compiler for debug builds." FORCE)
    set(CMAKE_CXX_FLAGS_DEBUG "-g -O0" CACHE STRING "Flags used by the CXX compiler for debug builds." FORCE)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    message(STATUS "Debug build type detected. Enabling sanitizers.")
    if(NOT MSVC)
        set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -fsanitize=address -fsanitize=undefined" CACHE STRING "Flags used by the C compiler for debug builds." FORCE)
        set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fsanitize=undefined" CACHE STRING "Flags used by the CXX compiler for debug builds." FORCE)
        set(CMAKE_EXE_LINKER_FLAGS_DEBUG "-fsanitize=address -fsanitize=undefined" CACHE STRING "Linker flags for debug builds." FORCE)
    endif()
endif()

# Define NOMINMAX for all targets on Windows to prevent <windows.h> from defining min/max macros.
if(MSVC)
    add_compile_definitions(NOMINMAX)
endif()

# Set the output directories. ALL binaries (executables AND libraries) go to the project's bin dir!
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/bin")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/bin")


include_directories(src)


# --- Common Utility Library (STATIC) ---
add_library(stax_common STATIC
    src/stax_common/roaring.cpp
    src/stax_common/os_file_extensions.cpp
)
target_include_directories(stax_common PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
)

if(NOT MSVC)
    target_compile_options(stax_common PRIVATE
        -Wno-unused-result -Wno-unused-variable -Wno-pointer-sign
    )
endif()

find_package(Threads REQUIRED)
target_link_libraries(stax_common PUBLIC Threads::Threads)


# --- Stax Block 1: The Core Engine (Header-Only INTERFACE library) ---
add_library(stax_core INTERFACE)
target_link_libraries(stax_core INTERFACE stax_common)


# --- Stax Block 2: The Transaction Layer (Header-Only) ---
# This library now only provides headers (TxnContext, TransactionBatch)
# and is defined as an INTERFACE library, which doesn't produce a binary file.
add_library(stax_tx INTERFACE)
target_link_libraries(stax_tx INTERFACE stax_core)


# --- Stax Block 3: The Database Management Layer ---
add_library(stax_db STATIC
    src/stax_db/db.cpp
    src/stax_db/query.cpp
    src/stax_core/stax_tree.cpp
    src/stax_db/statistics.cpp
    src/stax_core/node_allocator.cpp
)
target_link_libraries(stax_db PUBLIC stax_tx stax_common)


# --- Stax Block 4: The Graph Engine ---
add_library(stax_graph STATIC
    src/stax_graph/graph_engine.cpp
)
target_link_libraries(stax_graph PUBLIC stax_db)


# --- Stax Block 5: The C FFI API Layer ---
add_library(staxdb_api_lib SHARED
    src/stax_api/staxdb_api.cpp
)
set_target_properties(staxdb_api_lib PROPERTIES OUTPUT_NAME "staxdb")
# staxdb_api uses symbols from stax_graph (for global_id_map) and stax_db (for global_transaction_factory)
# and needs to link against them directly to resolve these symbols.
target_link_libraries(staxdb_api_lib PRIVATE stax_graph stax_db)


# --- NEW: INTERFACE Library for Node.js Addon ---
# This target doesn't build anything itself. It exists to define the
# necessary include paths for an external build system like node-gyp.
add_library(stax_node_api INTERFACE)
target_include_directories(stax_node_api INTERFACE
    src
)
# The node addon will depend on the headers from all these components.
target_link_libraries(stax_node_api INTERFACE stax_graph stax_db stax_tx stax_core stax_common)


# --- Benchmark Runner Executable ---
add_executable(stax_benchmark
    src/main.cpp
    src/benchmarks/tpcc.cpp
    src/stax_api/tcp_server.cpp # Added to stax_benchmark sources

)
target_link_libraries(stax_benchmark PRIVATE stax_graph stax_common)

# --- Boost.Asio Integration ---
find_package(Boost REQUIRED COMPONENTS system)
# Corrected include directory for Asio
include_directories(third_party/asio/include/boost/)

# Link benchmark runner to Boost
target_link_libraries(stax_benchmark PRIVATE Boost::system)
# On Unix-like systems, real-time extensions for clocks/threads may be needed for Asio
if(UNIX AND NOT APPLE)
    target_link_libraries(stax_benchmark PRIVATE rt) # Linked to stax_benchmark
endif()

# Removed separate executables for TCP server and client.
# Their source files are now part of stax_benchmark.

# --- INSTALL STEP ---
# This will install the shared library to <prefix>/lib and executables to <prefix>/bin
install(TARGETS staxdb_api_lib stax_benchmark
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
)