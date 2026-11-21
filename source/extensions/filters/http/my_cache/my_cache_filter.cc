#include "source/extensions/filters/http/my_cache/my_cache_filter.h"

#include "source/common/buffer/buffer_impl.h"
#include "source/common/http/header_map_impl.h"
#include "source/common/http/headers.h"

#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/strip.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {

namespace{
//-------SOURCE: CLAUDE---------
// Returns max-age in seconds, or nullopt if the header doesn't have one.
absl::optional<uint64_t> parseMaxAge(absl::string_view cache_control) {
  for (absl::string_view directive : absl::StrSplit(cache_control, ',')) {
    directive = absl::StripAsciiWhitespace(directive);
    if (absl::ConsumePrefix(&directive, "max-age=")) {
      uint64_t seconds;
      if (absl::SimpleAtoi(directive, &seconds)) {
        return seconds;
      }
    }
  }
  return absl::nullopt;
}
//------------------------------
}

Http::FilterHeadersStatus MyCacheFilter::decodeHeaders(Http::RequestHeaderMap& headers,
                                                       bool) {

  if (headers.getMethodValue() != "GET")
    return Http::FilterHeadersStatus::Continue;

  std::string key = absl::StrCat(headers.getHostValue(), headers.getPathValue());

  EntrySharedPtr entry =
      cache_->lookup(key, decoder_callbacks_->dispatcher().timeSource().monotonicTime());

  if (entry) {
    // HIT
    auto headers = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(*entry->headers);
    headers->addCopy(Http::LowerCaseString("my-cache"), "HIT");

    decoder_callbacks_->encodeHeaders(std::move(headers), entry->body.empty(), "HIT");

    if (!entry->body.empty()) {
      Buffer::OwnedImpl body(entry->body);
      decoder_callbacks_->encodeData(body, true);
    }
    return Http::FilterHeadersStatus::StopIteration;
  }

  // MISS
  key_ = key; // save for encoding
  caching_ = true;
  return Http::FilterHeadersStatus::Continue;
}

Http::FilterHeadersStatus MyCacheFilter::encodeHeaders(Http::ResponseHeaderMap& responseHeaders,
                                                      bool) {
  if(!caching_)return Http::FilterHeadersStatus::Continue;
  if(responseHeaders.getStatusValue() != "200"){
    caching_ = false;
    return Http::FilterHeadersStatus::Continue;
  }
                                                  

  auto ccRes = responseHeaders.get(Http::LowerCaseString("cache-control"));

  absl::string_view cache_control = ccRes.empty() ? "" : ccRes[0]->value().getStringView();
  std::optional<uint64_t> maxAge = parseMaxAge(cache_control);
  
  if(absl::StrContains(cache_control, "no-store")
  || absl::StrContains(cache_control, "private")
  || maxAge == std::nullopt) {
    caching_ = false;
    return Http::FilterHeadersStatus::Continue;
  }

  pending_headers_ = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(responseHeaders);
  expires_at_ = encoder_callbacks_->dispatcher().timeSource().monotonicTime()
              + std::chrono::seconds(*maxAge);

  return Http::FilterHeadersStatus::Continue;
}

Http::FilterDataStatus MyCacheFilter::encodeData(Buffer::Instance& buffer,
                                                    bool end_stream){
  if(!caching_)return Http::FilterDataStatus::Continue;

  pending_body_.append(buffer.toString());

  if (end_stream && caching_) {
    auto entry = std::make_shared<Entry>();
    entry->headers = std::move(pending_headers_);
    entry->body = std::move(pending_body_);
    entry->expires_at = expires_at_;
    cache_->insert(key_, std::move(entry));
  }

  return Http::FilterDataStatus::Continue;

}

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
