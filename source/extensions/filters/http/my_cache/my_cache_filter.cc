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
  if(headers.getMethodValue() != "GET") 
    return Http::FilterHeadersStatus::Continue;

  if(!isCacheable(headers)) return Http::FilterHeadersStatus::Continue;

  std::string key = absl::StrCat(headers.getHostValue(), headers.getPathValue());

  LookupResult lookup_result =
      cache_->lookup_or_follow(key, 
                               decoder_callbacks_->dispatcher().timeSource().monotonicTime(),
                               weak_from_this(),
                               &decoder_callbacks_->dispatcher());
  switch (lookup_result.kind)
  {
    case LookupResult::Kind::Hit:
    {
      auto headers = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(*lookup_result.entry->headers);
      headers->addCopy(Http::LowerCaseString("my-cache"), "HIT");

      decoder_callbacks_->encodeHeaders(std::move(headers), lookup_result.entry->body.empty(), "HIT");

      if (!lookup_result.entry->body.empty()) {
        Buffer::OwnedImpl body(lookup_result.entry->body);
        decoder_callbacks_->encodeData(body, true);
      }
      return Http::FilterHeadersStatus::StopIteration;
    }

    case LookupResult::Kind::Leader:
    {
      Upstream::ThreadLocalCluster* cluster = cluster_manager_.getThreadLocalCluster("origin");
      if(cluster == nullptr
      || !lookup_result.in_flight->start(*cluster, Http::createHeaderMap<Http::RequestHeaderMapImpl>(headers))){
        
        lookup_result.in_flight->fail();
      }
      return Http::FilterHeadersStatus::StopIteration;
    }
    case LookupResult::Kind::Follower:
    {
      return Http::FilterHeadersStatus::StopIteration;
    }
  }
  return Http::FilterHeadersStatus::StopIteration; //OR PANIC
}

void MyCacheFilter::onHeaders(Http::ResponseHeaderMapPtr headers, bool end_stream){
  if(destroyed_ || done_) return;
  decoder_callbacks_->encodeHeaders(std::move(headers), end_stream, "my_cache_miss");
}

void MyCacheFilter::onData(const std::string& data, bool end_stream){
  if(destroyed_ || done_) return;
  Buffer::OwnedImpl buffer(data);
  decoder_callbacks_->encodeData(buffer, end_stream);
}

void MyCacheFilter::onTrailers(Http::ResponseTrailerMapPtr trailers){
  if(destroyed_ || done_) return;
  decoder_callbacks_->encodeTrailers(std::move(trailers));
}

void MyCacheFilter::onComplete(){
  if(destroyed_ || done_) return;
  done_ = true;
  Buffer::OwnedImpl empty_body("");
  decoder_callbacks_->encodeData(empty_body, true);
}
void MyCacheFilter::onFail(){
  if(destroyed_ || done_) return;
  done_ = true;
  decoder_callbacks_->sendLocalReply(Http::Code::ServiceUnavailable,
                                       "upstream fetch failed",
                                       nullptr, std::nullopt,
                                       "upstream_fetch_failed");
}
void MyCacheFilter::onReset(){
  if(destroyed_ || done_) return;
  done_ = true;
  decoder_callbacks_->resetStream();
}

void MyCacheFilter::onDestroy(){ //called by dispatcher?
  destroyed_ = true;
  //because envoy call onDestroy() first and deletes the object later,
  //so callbacks would be invalid
}


} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
