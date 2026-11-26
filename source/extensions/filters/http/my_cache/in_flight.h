// One origin fetch, shared by all clients requesting the same URL ("followers").
// The fetch is independent of any single client: the request that created it is
// just the first follower, and the fetch continues if clients disconnect.
//
// Threads: all AsyncClient callbacks (onHeaders/onData/onTrailers/onComplete/
// onReset) run on the leader's worker; addFollower() runs on the joining
// client's worker. Clients are never touched directly - everything is posted to
// each follower's own dispatcher. Posts happen while holding mutex_, so a late
// joiner's catch-up can never be reordered with live notifications.
//
// Lifetime: owned by MyCache::in_flight_map_ while the fetch runs. Terminal
// callbacks pin the object (shared_from_this), because removing the map entry
// may drop its last owner mid-function. Exactly one terminal event per fetch:
// state_ == Done guards onComplete / onReset / fail.
//
// Modes:
//   pull (default) - the body goes into a SharedBody; clients read it at their
//                    own pace.
//   push           - Content-Length > max_object_bytes: nothing is stored; chunks
//                    are pushed to clients that joined before the headers, and
//                    later joiners are rejected (no stored beginning to catch up from).
// Non-shareable responses (private, Set-Cookie): only the first follower receives
// it; the others are released to the router (continueDecoding) and fetch alone.

#pragma once
#include "envoy/event/dispatcher.h"
#include "envoy/http/async_client.h"
#include "envoy/http/filter.h"
#include "envoy/upstream/thread_local_cluster.h"

#include "source/common/http/header_map_impl.h"
#include "source/extensions/filters/http/my_cache/my_cache.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {

class MyCacheFilter;

class InFlight : public Http::AsyncClient::StreamCallbacks,
                 public std::enable_shared_from_this<InFlight> {

  enum class State { Waiting, HeadersArrived, Done };

  struct Follower {
    std::weak_ptr<MyCacheFilter> filter;
    Event::Dispatcher* dispatcher;
  };

  template <class Fn> void postToFollowers(const std::vector<Follower>& followers, Fn fn);

public:
  InFlight(std::weak_ptr<MyCache> cache, const std::string& key, TimeSource& time_source,
           const uint64_t max_object_bytes);

  ~InFlight() override;

  bool start(Upstream::ThreadLocalCluster& cluster, Http::RequestHeaderMapPtr headers);

  void onHeaders(Http::ResponseHeaderMapPtr&& headers, bool end_stream) override;
  void onData(Buffer::Instance& data, bool end_stream) override;
  void onTrailers(Http::ResponseTrailerMapPtr&& trailers) override;
  void onComplete() override;
  void onReset() override;
  void fail();

  bool addFollower(std::weak_ptr<MyCacheFilter> filter, Event::Dispatcher* dispatcher);

private:
  std::weak_ptr<MyCache> cache_;
  std::string key_;
  TimeSource& time_source_;
  SharedBodyPtr body_;
  const uint64_t max_object_bytes_;
  std::shared_ptr<Entry> entry_ = nullptr;

  bool cacheable_ = false;
  bool shareable_ = false;
  bool destroying_ = false;
  bool push_mode_ = false; // false implies pull mode
  State state_ = State::Waiting;

  Http::RequestHeaderMapPtr request_headers_;
  Http::ResponseHeaderMapPtr response_headers_;
  std::vector<Follower> followers_;
  Http::AsyncClient::Stream* stream_{nullptr};

  absl::Mutex mutex_;
};

using InFlightSharedPtr = std::shared_ptr<InFlight>;

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
