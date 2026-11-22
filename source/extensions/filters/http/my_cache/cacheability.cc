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
  for (absl::string_view directive : absl::StrSplit(cache_control, ',')) {
    directive = absl::StripAsciiWhitespace(directive);
    if (absl::ConsumePrefix(&directive, "max-age=")) {
      uint64_t seconds;
      if (absl::SimpleAtoi(directive, &seconds)) {
        return seconds;
      }
    }
  }
  return std::nullopt;
}
//------------------------------
}


namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {

std::optional<std::chrono::seconds> cacheableFor(const Http::ResponseHeaderMap& headers)
{
  if(headers.getStatusValue() != "200")return std::nullopt;
                                                  

  auto cc_res = headers.get(Http::LowerCaseString("cache-control"));

  absl::string_view cache_control = cc_res.empty() ? "" : cc_res[0]->value().getStringView();
  std::optional<uint64_t> max_age = parseMaxAge(cache_control);
  
  if(absl::StrContains(cache_control, "no-store")
  || absl::StrContains(cache_control, "private")
  || max_age == std::nullopt)
      return std::nullopt;
  
  return std::chrono::seconds(*max_age);

}

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
