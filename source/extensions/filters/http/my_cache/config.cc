#include "source/extensions/filters/http/my_cache/config.h"

#include <chrono>
#include <string>

#include "envoy/extensions/filters/http/my_cache/v3/my_cache.pb.h"
#include "envoy/extensions/filters/http/my_cache/v3/my_cache.pb.validate.h"
#include "envoy/registry/registry.h"
#include "envoy/upstream/cluster_manager.h"

#include "source/common/protobuf/utility.h"
#include "source/extensions/filters/http/my_cache/in_flight.h"
#include "source/extensions/filters/http/my_cache/my_cache.h"
#include "source/extensions/filters/http/my_cache/my_cache_filter.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {

absl::StatusOr<Envoy::Http::FilterFactoryCb>
MyCacheFilterFactory::createHttpFilterFactoryFromProtoTyped(
    const envoy::extensions::filters::http::my_cache::v3::MyCache& proto_config,
    Server::Configuration::ServerFactoryContext& context,
    Server::Configuration::ExtraFactoryContext&) {

  const uint64_t max_total =
      PROTOBUF_GET_WRAPPED_OR_DEFAULT(proto_config, max_total_bytes, 256 * 1024 * 1024);
  const uint64_t max_object =
      PROTOBUF_GET_WRAPPED_OR_DEFAULT(proto_config, max_object_bytes, 32 * 1024 * 1024);
  MyCacheSharedPtr cache = std::make_shared<MyCache>(max_total, max_object);

  Upstream::ClusterManager& cluster_manager = context.clusterManager();

  return [cache, &cluster_manager](Http::FilterChainFactoryCallbacks& callbacks) {
    callbacks.addStreamFilter(std::make_shared<MyCacheFilter>(cache, cluster_manager));
  };
}
REGISTER_FACTORY(MyCacheFilterFactory, Server::Configuration::NamedHttpFilterConfigFactory);

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
