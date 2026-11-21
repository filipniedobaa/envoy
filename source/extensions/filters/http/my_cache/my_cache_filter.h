#pragma once

#include "source/extensions/filters/http/common/pass_through_filter.h"
#include "source/extensions/filters/http/my_cache/my_cache.h"


namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {


class MyCacheFilter : public Http::PassThroughFilter{
public:
  explicit MyCacheFilter(MyCacheSharedPtr cache) : cache_(cache){}

  Http::FilterHeadersStatus decodeHeaders(
    Http::RequestHeaderMap& headers,
    bool end_stream) override;

  Http::FilterHeadersStatus encodeHeaders(
    Http::ResponseHeaderMap& headers,
    bool end_stream) override;

  Http::FilterDataStatus encodeData(
    Buffer::Instance& data, 
    bool end_stream) override;

private:

  MyCacheSharedPtr cache_;
  std::string key_;
  bool caching_ = false;
  MonotonicTime expires_at_;
  Http::ResponseHeaderMapPtr pending_headers_;
  std::string pending_body_;

};

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
