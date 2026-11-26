// Per-request filter. Everything runs on the worker that owns the client's
// connection, so its members need no locks.
// decodeHeaders asks MyCache what to do (Hit / Leader / Follower / NonShareable).
// Hits and followers are served the same way, by pull(): send new chunks from
// the SharedBody starting at cursor_, pause while the client's buffer is above
// its high watermark, resume on the low watermark. Memory per client therefore
// stays around one watermark, and each body exists only once.
// Chunks >= kZeroCopyThreshold are sent zero-copy (a buffer fragment pointing
// into the SharedBody, keeping the chunk alive); smaller ones are copied,
// which is cheaper than allocating a fragment.
// Push mode (oversized objects): chunks arrive via onUpstreamData. A client
// pushed more than the cache's max object size while above its watermark
// is reset, which bounds memory per slow client.
// destroyed_: Envoy called onDestroy - callbacks are invalid, even though the
//             object may still exist (deferred deletion).
// done_:      this stream has ended - never write to it again.

#pragma once

#include "envoy/upstream/cluster_manager.h"

#include "source/extensions/filters/http/common/pass_through_filter.h"
#include "source/extensions/filters/http/my_cache/in_flight.h"
#include "source/extensions/filters/http/my_cache/my_cache.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {

constexpr size_t kZeroCopyThreshold =
    16 * 1024; // 16KiB; below this, copying beats a fragment (not tuned)

class MyCacheFilter : public Http::PassThroughFilter,
                      public Http::DownstreamWatermarkCallbacks,
                      public std::enable_shared_from_this<MyCacheFilter> {
public:
  explicit MyCacheFilter(MyCacheSharedPtr cache, Upstream::ClusterManager& cluster_manager)
      : cache_(cache), cluster_manager_(cluster_manager) {}

  Http::FilterHeadersStatus decodeHeaders(Http::RequestHeaderMap& headers,
                                          bool end_stream) override;

  void onDestroy() override;

  void onUpstreamHeaders(Http::ResponseHeaderMapPtr headers, SharedBodyPtr body);
  void onUpstreamComplete();
  void onUpstreamData(std::shared_ptr<const std::string> chunk);
  void onBodyAvailable();
  void onTrailers(Http::ResponseTrailerMapPtr trailers);
  void onUpstreamFail();
  void abortStream();
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

  uint64_t bytes_pushed_while_high_watermark_ = 0;
  uint32_t high_watermark_counter_ = 0;
  bool watermark_registered_ = false;
};

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
