#pragma once
#include "envoy/http/async_client.h"
#include "source/extensions/filters/http/my_cache/my_cache.h"
#include "source/common/http/header_map_impl.h"
#include "envoy/http/filter.h"
#include "envoy/upstream/thread_local_cluster.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {

class InFlight : public Http::AsyncClient::StreamCallbacks{
public:
  InFlight(Http::StreamDecoderFilterCallbacks& callbacks,
           MyCacheSharedPtr cache,
           std::string key);

  ~InFlight() override;

  bool start(Upstream::ThreadLocalCluster& cluster,
             Http::RequestHeaderMapPtr headers);

  void onHeaders(Http::ResponseHeaderMapPtr&& headers, bool end_stream) override;
  void onData(Buffer::Instance& data, bool end_stream) override;
  void onTrailers(Http::ResponseTrailerMapPtr&& trailers) override;
  void onComplete() override;
  void onReset() override;

private:
    MyCacheSharedPtr cache_;
    std::string key_;
    std::shared_ptr<Entry> entry_ = nullptr;

    bool cacheable_ = false;
    bool destroying_ = false;
    bool headers_sent_ = false;

    Http::RequestHeaderMapPtr request_headers_;
    Http::StreamDecoderFilterCallbacks& callbacks_;
    Http::AsyncClient::Stream* stream_{nullptr};

    

};

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
