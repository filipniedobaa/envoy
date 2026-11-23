#pragma once
#include <optional>
#include <chrono>

#include "envoy/http/header_map.h"


namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {

bool isCacheable(const Http::RequestHeaderMap& headers);

// Returns time to live, or nullopt if it cant be cached
std::optional<std::chrono::seconds> cacheableFor(const Http::ResponseHeaderMap& headers);

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
