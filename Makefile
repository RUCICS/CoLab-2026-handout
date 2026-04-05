BUILD_DIR ?= build
OUT_DIR ?= out
RUNNER_BIN := $(BUILD_DIR)/benchmark/runner
STABLE_RUNNER := $(OUT_DIR)/runner
JOBS ?= $(shell sh -c 'n=$$(nproc 2>/dev/null || echo 8); [ "$$n" -gt 32 ] && n=32; echo $$n')
TEST_JOBS ?= $(shell sh -c 'n=$$(nproc 2>/dev/null || echo 8); [ "$$n" -gt 16 ] && n=16; echo $$n')
BENCH_ARGS ?=

.PHONY: runner test bench grade clean

$(BUILD_DIR)/CMakeCache.txt:
	@cmake -S . -B $(BUILD_DIR)

$(RUNNER_BIN): $(BUILD_DIR)/CMakeCache.txt
	@cmake --build $(BUILD_DIR) --parallel $(JOBS) --target runner

$(STABLE_RUNNER): $(RUNNER_BIN)
	@mkdir -p $(OUT_DIR)
	@cp $(RUNNER_BIN) $(STABLE_RUNNER)

runner: $(STABLE_RUNNER)

test: $(BUILD_DIR)/CMakeCache.txt
	@cmake --build $(BUILD_DIR) --parallel $(JOBS)
	@ctest --test-dir $(BUILD_DIR) --output-on-failure --parallel $(TEST_JOBS)
	@python3 -m unittest discover -s tests/tools -p '*_test.py'

bench: runner
	@python3 tools/bench.py $(if $(strip $(BENCH_ARGS)),$(BENCH_ARGS),list --suite public)

grade: $(BUILD_DIR)/CMakeCache.txt runner
	@cmake --build $(BUILD_DIR) --parallel $(JOBS)
	@python3 tools/bench.py grade

clean:
	@rm -rf $(OUT_DIR)
	@if [ -f "$(BUILD_DIR)/CMakeCache.txt" ]; then cmake --build $(BUILD_DIR) --target clean; fi
