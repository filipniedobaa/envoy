#pragma once

#include "source/extensions/filters/http/common/pass_through_filter.h"
#include "source/extensions/filters/http/my_cache/my_cache.h"
#include "source/extensions/filters/http/my_cache/in_flight.h"
#include "envoy/upstream/cluster_manager.h"


namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {


class MyCacheFilter : public Http::PassThroughFilter, 
                      public std::enable_shared_from_this<MyCacheFilter>{
public:
  explicit MyCacheFilter(MyCacheSharedPtr cache,
                         Upstream::ClusterManager& cluster_manager)
                         : cache_(cache), cluster_manager_(cluster_manager){}

  Http::FilterHeadersStatus decodeHeaders(
    Http::RequestHeaderMap& headers,
    bool end_stream) override;

  void onDestroy() override;
  
  void onHeaders(Http::ResponseHeaderMapPtr headers, bool end_stream);
  void onComplete();
  void onData(const std::string& data, bool end_stream);
  void onTrailers(Http::ResponseTrailerMapPtr trailers);
  void onFail();
  void onReset();
  void onBypass();

private:
  MyCacheSharedPtr cache_;
  Upstream::ClusterManager& cluster_manager_;
  bool destroyed_ = false;
  bool done_ = false;
};

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
