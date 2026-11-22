#include "envoy/http/async_client.h"
#include "source/extensions/filters/http/my_cache/my_cache.h"
#include "source/extensions/filters/http/my_cache/cacheability.h"
#include "envoy/upstream/thread_local_cluster.h"
#include "source/extensions/filters/http/my_cache/in_flight.h"
#include "source/common/http/header_map_impl.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {

InFlight::InFlight(Http::StreamDecoderFilterCallbacks& callbacks,
          MyCacheSharedPtr cache,
          std::string key):
          cache_(cache), key_(std::move(key)),
          callbacks_(callbacks){}

InFlight::~InFlight(){
  destroying_ = true;
  if(stream_ != nullptr) stream_->reset();
}

bool InFlight::start(Upstream::ThreadLocalCluster& cluster,
                     Http::RequestHeaderMapPtr headers){
  Http::AsyncClient::StreamOptions stream_options;
  stream_options.setTimeout(std::chrono::milliseconds(30000));

  stream_ = cluster.httpAsyncClient().start(*this, stream_options);
  if(!stream_) return false;

  request_headers_ = std::move(headers);
  stream_->sendHeaders(*request_headers_, true);
  return true;
}

void InFlight::onHeaders(Http::ResponseHeaderMapPtr&& headers, bool end_stream){
  std::optional<std::chrono::seconds> ttl = cacheableFor(*headers);
  if(ttl != std::nullopt){
    cacheable_ = true;

    entry_ = std::make_shared<Entry>();
    entry_->headers = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(*headers);
    entry_->expires_at = callbacks_.dispatcher().timeSource().monotonicTime()
                       + *ttl;
  }

  callbacks_.encodeHeaders(std::move(headers), end_stream, "my_cache_miss");
  headers_sent_ = true;
}

void InFlight::onData(Buffer::Instance& data, bool end_stream){
  if(cacheable_) entry_->body.append(data.toString());
  callbacks_.encodeData(data, end_stream);
}

void InFlight::onTrailers(Http::ResponseTrailerMapPtr&& trailers){
  cacheable_ = false;
  callbacks_.encodeTrailers(std::move(trailers));
}

void InFlight::onComplete(){
  stream_ = nullptr;
  if(cacheable_) cache_->insert(key_, std::move(entry_));
}

void InFlight::onReset(){
  stream_ = nullptr;
  if(destroying_)return;
  if(!headers_sent_) callbacks_.sendLocalReply(Http::Code::ServiceUnavailable,
                                       "upstream fetch failed",
                                       nullptr, std::nullopt, "my_cache_upstream_reset"); //send 503
  else callbacks_.resetStream();
}


} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy