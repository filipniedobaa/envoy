#pragma once
#include <chrono>
#include <optional>

#include "envoy/http/header_map.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {

struct LifeTimeAndAge {
  std::chrono::seconds lifetime;
  std::chrono::seconds age;
};

bool isShareable(const Http::ResponseHeaderMap& headers);
bool requestMayCache(const Http::RequestHeaderMap& headers);

// Returns lifetime and age, or nullopt if it cant be cached
std::optional<LifeTimeAndAge> cacheableFor(const Http::ResponseHeaderMap& headers);

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
