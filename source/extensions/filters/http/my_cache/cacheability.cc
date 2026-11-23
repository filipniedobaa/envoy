#include <optional>
#include <chrono>

#include "envoy/http/header_map.h"
#include "source/extensions/filters/http/my_cache/cacheability.h"

#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "absl/strings/strip.h"

namespace{
//-------SOURCE: CLAUDE---------
// Returns max-age in seconds, or nullopt if the header doesn't have one.
std::optional<uint64_t> parseMaxAge(absl::string_view cache_control) {
  bool foundLocalMaxAge = false;
  uint64_t localMaxAge;
  for (absl::string_view directive : absl::StrSplit(cache_control, ',')) {
    directive = absl::StripAsciiWhitespace(directive);

    if(absl::ConsumePrefix(&directive, "s-maxage=")){
      uint64_t seconds;
      if (absl::SimpleAtoi(directive, &seconds)) {
        return seconds;
      }
    }
    else if (absl::ConsumePrefix(&directive, "max-age=")) {
      if (absl::SimpleAtoi(directive, &localMaxAge)) {
        foundLocalMaxAge = true;
      }
    }
  }
  if(foundLocalMaxAge) return localMaxAge;
  return std::nullopt;
}
//------------------------------
}


namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {

bool isCacheable(const Http::RequestHeaderMap& headers){
  if(!headers.get(Http::LowerCaseString("Range")).empty()
  || !headers.get(Http::LowerCaseString("If-None-Match")).empty()
  || !headers.get(Http::LowerCaseString("If-Modified-Since")).empty()
  || !headers.get(Http::LowerCaseString("If-Match")).empty()
  || !headers.get(Http::LowerCaseString("If-Unmodified-Since")).empty()
  || !headers.get(Http::LowerCaseString("If-Range")).empty()
  || !headers.get(Http::LowerCaseString("Authorization")).empty()
  || !headers.get(Http::LowerCaseString("Cookie")).empty()
  || !headers.get(Http::LowerCaseString("Range")).empty()) return false;
  
  return true;
}

std::optional<std::chrono::seconds> cacheableFor(const Http::ResponseHeaderMap& headers)
{
  if(headers.getStatusValue() != "200")return std::nullopt;

  if(!headers.get(Http::LowerCaseString("Set-Cookie")).empty()) return std::nullopt;
                                                  
  auto cc_res = headers.get(Http::LowerCaseString("cache-control"));
  

  absl::string_view raw = cc_res.empty() ? "" : cc_res[0]->value().getStringView();
  std::string cache_control = absl::AsciiStrToLower(raw);

  std::optional<uint64_t> max_age = parseMaxAge(cache_control);
  
  if(absl::StrContains(cache_control, "no-store")
  || absl::StrContains(cache_control, "private")
  || absl::StrContains(cache_control, "no-cache")
  || max_age == std::nullopt)
      return std::nullopt;
  
  return std::chrono::seconds(*max_age);

}

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
