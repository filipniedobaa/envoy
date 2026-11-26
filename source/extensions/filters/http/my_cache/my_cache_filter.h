#pragma once

#include "source/extensions/filters/http/common/pass_through_filter.h"
#include "source/extensions/filters/http/my_cache/my_cache.h"
#include "source/extensions/filters/http/my_cache/in_flight.h"
#include "envoy/upstream/cluster_manager.h"


namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {

/*class Fragment : public Buffer::BufferFragment{
public:
  explicit Fragment(std::shared_ptr<const std::string> chunk)
    : chunk_(std::move(chunk)){}

  const void* data() const override { return chunk_; }
  size_t size() const override { return chunk_->size(); }
  void done() override {
    if (releasor_) {
      releasor_(data_, size_, this);
    }
  }
private:
  const void* const  chunk_;
};*/

class MyCacheFilter : public Http::PassThroughFilter, 
                      public Http::DownstreamWatermarkCallbacks,
                      public std::enable_shared_from_this<MyCacheFilter>{
public:
  explicit MyCacheFilter(MyCacheSharedPtr cache,
                         Upstream::ClusterManager& cluster_manager)
                         : cache_(cache), cluster_manager_(cluster_manager){}

  Http::FilterHeadersStatus decodeHeaders(
    Http::RequestHeaderMap& headers,
    bool end_stream) override;

  void onDestroy() override;
  
  void onUpstreamHeaders(Http::ResponseHeaderMapPtr headers, SharedBodyPtr body);
  void onComplete();
  void onUpstreamData(std::shared_ptr<const std::string> chunk);
  void onBodyAvailable();
  void onTrailers(Http::ResponseTrailerMapPtr trailers);
  void onUpstreamFail();
  void onReset();
  void onBypass();
  void pull();

  void onAboveWriteBufferHighWatermark() override;
  void onBelowWriteBufferLowWatermark() override;

private:
  MyCacheSharedPtr cache_;
  SharedBodyPtr body_;
  size_t cursor_ = 0;
  Upstream::ClusterManager& cluster_manager_;
  bool destroyed_ = false;
  bool done_ = false;

  uint64_t bytes_pushed_while_high_watermark = 0;
  uint32_t high_watermark_counter_ = 0;
  bool watermark_registered_ = false;
};

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
