.PHONY: all release debug run test clean build-cpp bench profile package publish

# --- Configuration ---
ROOT_DIR          := $(shell pwd)
BUILD_DIR_RELEASE = build/release
BUILD_DIR_DEBUG   = build/debug
OUTPUT_DIR        = $(ROOT_DIR)/bin

# C++ Executable
CPP_BENCHMARK_NAME = stax_benchmark

# --- Targets ---

# Default target: build C++ release and install Node.js dependencies
all: build-cpp
	@echo "--- Installing Node.js dependencies and building native addon... ---"
	@cd stax_js && npm install

# Build only the C++ part (Release)
build-cpp:
	@echo "--- Ensuring C++ Release build is up to date... ---"
	@mkdir -p $(OUTPUT_DIR)
	@mkdir -p $(BUILD_DIR_RELEASE)
	@cmake -S . -B $(BUILD_DIR_RELEASE) -DCMAKE_BUILD_TYPE=Release
	@cmake --build $(BUILD_DIR_RELEASE)
	@echo "--- C++ library and benchmark written to $(OUTPUT_DIR) by CMake ---"

# Target to build the full Release version
release: all

# Target to build the Debug version
debug:
	@echo "--- Ensuring C++ Debug build is up to date... ---"
	@mkdir -p $(OUTPUT_DIR)
	@mkdir -p $(BUILD_DIR_DEBUG)
	@cmake -S . -B $(BUILD_DIR_DEBUG) -DCMAKE_BUILD_TYPE=Debug
	@cmake --build $(BUILD_DIR_DEBUG)
	@echo "--- C++ debug binaries written to $(OUTPUT_DIR) by CMake ---"
	@echo "--- Installing Node.js dependencies and building native addon (debug)... ---"
	@cd stax_js && npm install --debug

# Target to run the benchmark (builds first if needed)
run: all bench

# Target to explicitly run the benchmark script
bench:
	@echo "--- Running Node.js Benchmark Script ---"
	@cd stax_js && npm run bench

# Target to explicitly run the Node.js test script
test:
	@echo "--- Running Node.js Test Script ---"
	@cd stax_js && npm test

bench-core: build-cpp
	@echo "--- Running StaxCore Benchmark ---"
	@./bin/stax_core_benchmark

bench-wasm:
	@if ! command -v em++ >/dev/null 2>&1; then \
		echo "WARNING: em++ command not found. Skipping WASM benchmark."; \
		echo "Please make sure the Emscripten SDK is installed and activated."; \
	else \
		echo "--- Building and Running StaxCore WASM Benchmark ---"; \
		mkdir -p ./bin; \
		em++ -std=c++20 -O3 -DWASM_BUILD -sSINGLE_FILE=1 \
			-sALLOW_MEMORY_GROWTH=1 -sINITIAL_MEMORY=314572800 \
			-I staxcore/include -I staxcore/src \
			staxcore/src/staxcore.cpp staxcore/bench/stax_benchmark.cpp \
			-o ./bin/stax_core_benchmark.js; \
		echo "--- Running WASM benchmark with Node.js ---"; \
		node ./bin/stax_core_benchmark.js; \
	fi

# NEW: Target to explicitly run the Node.js profiling script
profile:
	@echo "--- Running Node.js Profiling Script ---"
	@cd stax_js && npm run profile

# NEW: Target to build and prepare the stax_js package for publishing
package:
	@echo "--- Cleaning previous builds ---"
	@$(MAKE) clean
	@echo "\n--- Building C++ Release Binaries ---"
	@$(MAKE) build-cpp
	@echo "\n--- Building Node.js Addon ---"
	@cd stax_js && npm install --ignore-scripts # Install dev deps without running postinstall
	@cd stax_js && npm exec -- node-gyp rebuild
	@echo "\n--- Staging Pre-built Binary for NPM Package ---"
	@PLATFORM_DIR=$$(node -p "process.platform + '-' + process.arch"); \
	if [ "$$(node -p 'process.platform')" = "linux" ]; then \
		LIBC=$$(ldd --version 2>/dev/null | grep -q 'musl' && echo 'musl' || echo 'glibc'); \
		PLATFORM_DIR="linux-$(shell uname -m)-$$LIBC"; \
	fi; \
	echo "Detected platform: $$PLATFORM_DIR"; \
	mkdir -p stax_js/prebuilds/$$PLATFORM_DIR; \
	cp stax_js/build/Release/staxdb.node stax_js/prebuilds/$$PLATFORM_DIR/staxdb.node; \
	echo "Binary copied to stax_js/prebuilds/$$PLATFORM_DIR/staxdb.node"; \
	echo "\n--- Package ready in 'stax_js' directory. You can now 'cd stax_js' and 'npm publish' ---"

# NEW: Target to package and publish the stax_js module to NPM
publish: package
	@echo "\n--- Publishing @matricom/staxdb to NPM ---"
	@cd stax_js && npm publish --access public
	@echo "\n--- Publish command executed successfully. ---"


# Target to clean all build artifacts
clean:
	@echo "Cleaning all build directories, executables, and Node modules..."
	@rm -rf build
	@rm -rf bin
	@rm -rf stax_js/node_modules
	@rm -rf stax_js/build
	@rm -f stax_js/package-lock.json