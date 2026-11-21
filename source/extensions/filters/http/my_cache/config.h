#pragma once

#include "envoy/extensions/filters/http/my_cache/v3/my_cache.pb.h"
#include "source/extensions/filters/http/my_cache/my_cache.h"

#include "source/extensions/filters/http/common/factory_base.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {

/**
 * Config registration for the MyCache filter.
 */
class MyCacheFilterFactory : public Common::UnifiedFactoryBase<
                                envoy::extensions::filters::http::my_cache::v3::MyCache,
                                envoy::extensions::filters::http::my_cache::v3::MyCachePerRoute> {
public:
  MyCacheFilterFactory() : UnifiedFactoryBase("envoy.filters.http.my_cache") {}


private:
  absl::StatusOr<Envoy::Http::FilterFactoryCb> createHttpFilterFactoryFromProtoTyped(
      const envoy::extensions::filters::http::my_cache::v3::MyCache& proto_config,
      Server::Configuration::ServerFactoryContext& context,
      Server::Configuration::ExtraFactoryContext& extra_context) override;

  /*absl::StatusOr<Router::RouteSpecificFilterConfigConstSharedPtr>
  createRouteSpecificFilterConfigTyped(
      const envoy::extensions::filters::http::my_cache::v3::MyCachePerRoute&,
      Server::Configuration::ServerFactoryContext&, ProtobufMessage::ValidationVisitor&) override;
    */
};



DECLARE_FACTORY(MyCacheFilterFactory);

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
