#pragma once

#include <cstdint>

namespace schedlab {

using DeviceId = uint32_t;

struct DeviceRequest {
  uint64_t service_units = 0;
  uint64_t opcode = 0;
  uint64_t arg0 = 0;
  uint64_t arg1 = 0;
};

struct DeviceResult {
  uint64_t status = 0;
  uint64_t value0 = 0;
  uint64_t value1 = 0;
};

} // namespace schedlab
