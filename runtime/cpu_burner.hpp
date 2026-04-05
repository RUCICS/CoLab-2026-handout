#pragma once

#include <cstdint>

namespace schedlab::runtime {

class CpuBurner {
public:
  void burn(uint64_t units) noexcept;

private:
  uint64_t state_ = 0x9e3779b97f4a7c15ULL;
};

} // namespace schedlab::runtime
