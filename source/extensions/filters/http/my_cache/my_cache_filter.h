#pragma once

#include "source/extensions/filters/http/common/pass_through_filter.h"
#include "source/extensions/filters/http/my_cache/my_cache.h"
#include "source/extensions/filters/http/my_cache/in_flight.h"
#include "envoy/upstream/cluster_manager.h"


namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {


class MyCacheFilter : public Http::PassThroughFilter{
public:
  explicit MyCacheFilter(MyCacheSharedPtr cache,
                         Upstream::ClusterManager& cluster_manager)
                         : cache_(cache), cluster_manager_(cluster_manager){}

  Http::FilterHeadersStatus decodeHeaders(
    Http::RequestHeaderMap& headers,
    bool end_stream) override;

private:

  MyCacheSharedPtr cache_;
  Upstream::ClusterManager& cluster_manager_;

  std::unique_ptr<InFlight> in_flight_ = nullptr;

};

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
