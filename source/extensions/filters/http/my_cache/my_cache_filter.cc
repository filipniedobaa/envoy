#include "source/extensions/filters/http/my_cache/my_cache_filter.h"
#include "source/extensions/filters/http/my_cache/cacheability.h"
#include "source/extensions/filters/http/my_cache/in_flight.h"

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
  Upstream::ThreadLocalCluster* cluster = cluster_manager_.getThreadLocalCluster("origin");
  if(cluster == nullptr){
    decoder_callbacks_->sendLocalReply(Http::Code::ServiceUnavailable,
                                       "decoding headers failed",
                                       nullptr, std::nullopt,
                                       "my_cache_no_cluster");
    return Http::FilterHeadersStatus::StopIteration;
  }

  in_flight_ = std::make_unique<InFlight>(*decoder_callbacks_, cache_, key);
  bool tmp = in_flight_->start(*cluster, Http::createHeaderMap<Http::RequestHeaderMapImpl>(headers));
  if(!tmp) decoder_callbacks_->sendLocalReply(Http::Code::ServiceUnavailable,
                                              "decoding headers failed",
                                              nullptr, std::nullopt,
                                              "my_cache_fetch_start_failed");
  return Http::FilterHeadersStatus::StopIteration;
}
} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
