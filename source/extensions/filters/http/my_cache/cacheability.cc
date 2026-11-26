#include "source/extensions/filters/http/my_cache/cacheability.h"

#include <chrono>
#include <optional>

#include "envoy/http/header_map.h"

#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "absl/strings/strip.h"

namespace {
//-------CLAUDE HELPED HERE---------
// Returns s-maxage or max-age in seconds if unavailable, or nullopt if the header doesn't neither.
std::optional<uint64_t> parseMaxAge(absl::string_view cache_control) {
  std::optional<uint64_t> localMaxAge;
  for (absl::string_view directive : absl::StrSplit(cache_control, ',')) {
    directive = absl::StripAsciiWhitespace(directive);

    if (absl::ConsumePrefix(&directive, "s-maxage=")) {
      uint64_t seconds;
      if (absl::SimpleAtoi(directive, &seconds)) {
        return seconds;
      }
    } else if (absl::ConsumePrefix(&directive, "max-age=")) {
      uint64_t seconds;
      if (absl::SimpleAtoi(directive, &seconds))
        localMaxAge = seconds;
    }
  }
  return localMaxAge;
}
//----------------------------------
} // namespace

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {

bool isShareable(const Http::ResponseHeaderMap& headers) {
  static const Http::LowerCaseString kSetCookie("Set-Cookie");
  static const Http::LowerCaseString kCacheControl("cache-control");

  if (!headers.get(kSetCookie).empty())
    return false;

  auto cc_res = headers.get(kCacheControl);
  absl::string_view raw = cc_res.empty() ? "" : cc_res[0]->value().getStringView();
  std::string cache_control = absl::AsciiStrToLower(raw);

  if (absl::StrContains(cache_control, "private"))
    return false;

  return true;
}

bool requestMayCache(const Http::RequestHeaderMap& headers) {
  static const Http::LowerCaseString kRange("Range");
  static const Http::LowerCaseString kIfNoneMatch("If-None-Match");
  static const Http::LowerCaseString kIfModifiedSince("If-Modified-Since");
  static const Http::LowerCaseString kIfMatch("If-Match");
  static const Http::LowerCaseString kIfUnmodifiedSince("If-Unmodified-Since");
  static const Http::LowerCaseString kIfRange("If-Range");
  static const Http::LowerCaseString kAuthorization("Authorization");
  static const Http::LowerCaseString kCookie("Cookie");

  return headers.get(kRange).empty() && headers.get(kIfNoneMatch).empty() &&
         headers.get(kIfModifiedSince).empty() && headers.get(kIfMatch).empty() &&
         headers.get(kIfUnmodifiedSince).empty() && headers.get(kIfRange).empty() &&
         headers.get(kAuthorization).empty() && headers.get(kCookie).empty();
}

std::optional<LifeTimeAndAge> cacheableFor(const Http::ResponseHeaderMap& headers) {
  static const Http::LowerCaseString kSetCookie("Set-Cookie");
  static const Http::LowerCaseString kCacheControl("cache-control");
  static const Http::LowerCaseString kAge("Age");

  if (headers.getStatusValue() != "200")
    return std::nullopt;
  if (!headers.get(kSetCookie).empty())
    return std::nullopt;

  auto cc_res = headers.get(kCacheControl);
  auto age_res = headers.get(kAge);

  uint64_t age = 0;
  if (!age_res.empty()) {
    if (!absl::SimpleAtoi(age_res[0]->value().getStringView(), &age))
      return std::nullopt;
  }

  absl::string_view raw = cc_res.empty() ? "" : cc_res[0]->value().getStringView();
  std::string cache_control = absl::AsciiStrToLower(raw);

  std::optional<uint64_t> max_age = parseMaxAge(cache_control);

  if (absl::StrContains(cache_control, "no-store") || absl::StrContains(cache_control, "private") ||
      absl::StrContains(cache_control, "no-cache") || max_age == std::nullopt)
    return std::nullopt;

  if (max_age <= age)
    return std::nullopt;
  return {{std::chrono::seconds(*max_age), std::chrono::seconds(age)}};
}

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
