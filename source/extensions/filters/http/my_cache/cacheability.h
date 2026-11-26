// Caching rules (RFC 9111, simplified):
//   requestMayCache - requests whose answer depends on the client's own state
//                     (Range, conditional headers, Authorization, Cookie)
//                     bypass the cache entirely.
//   isShareable     - may concurrent requests share this response?
//                     Not for personal responses (private, Set-Cookie).
//   cacheableFor    - may it be stored, and for how long? Only 200 OK responses;
//                     s-maxage wins over max-age; no-store / no-cache / private /
//                     Set-Cookie are never stored; the lifetime is reduced by
//                     the origin's Age.

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
