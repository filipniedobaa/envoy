#include "source/extensions/filters/http/my_cache/config.h"

#include <chrono>
#include <string>

#include "envoy/extensions/filters/http/my_cache/v3/my_cache.pb.h"
#include "envoy/extensions/filters/http/my_cache/v3/my_cache.pb.validate.h"
#include "envoy/registry/registry.h"

#include "source/extensions/filters/http/my_cache/my_cache_filter.h"
#include "source/extensions/filters/http/my_cache/my_cache.h"
#include "envoy/upstream/cluster_manager.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {


absl::StatusOr<Envoy::Http::FilterFactoryCb> 
MyCacheFilterFactory::createHttpFilterFactoryFromProtoTyped(
      const envoy::extensions::filters::http::my_cache::v3::MyCache&,
      Server::Configuration::ServerFactoryContext& context,
      Server::Configuration::ExtraFactoryContext&) {
  
  MyCacheSharedPtr cache = std::make_shared<MyCache>();
  Upstream::ClusterManager& cluster_manager = context.clusterManager();

  return [cache, &cluster_manager](Http::FilterChainFactoryCallbacks& callbacks){
    callbacks.addStreamFilter(std::make_shared<MyCacheFilter>(cache, cluster_manager));
  };
  
}
REGISTER_FACTORY(MyCacheFilterFactory, Server::Configuration::NamedHttpFilterConfigFactory);

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
