#include "runtime/cpu_burner.hpp"

#include <cstdint>

namespace schedlab::runtime {

namespace {
constexpr uint64_t kOpsPerUnit = 128;
} // namespace

void CpuBurner::burn(uint64_t units) noexcept {
  uint64_t value = state_;
  for (uint64_t unit = 0; unit < units; ++unit) {
    for (uint64_t op = 0; op < kOpsPerUnit; ++op) {
      value ^= (value << 7) + 0x9e3779b97f4a7c15ULL + op + unit;
      value = (value >> 3) | (value << 61);
    }
  }
  state_ = value;
}

} // namespace schedlab::runtime
