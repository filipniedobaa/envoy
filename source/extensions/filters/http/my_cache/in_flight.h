#pragma once
#include "envoy/http/async_client.h"
#include "source/extensions/filters/http/my_cache/my_cache.h"
#include "source/common/http/header_map_impl.h"
#include "envoy/http/filter.h"
#include "envoy/upstream/thread_local_cluster.h"
#include "envoy/event/dispatcher.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {

class MyCacheFilter;

class InFlight : public Http::AsyncClient::StreamCallbacks,
                 public std::enable_shared_from_this<InFlight>{

  enum class State {Waiting, HeadersArrived, Done};

  struct Follower{
      std::weak_ptr<MyCacheFilter> filter;
      Event::Dispatcher* dispatcher;
  };

  template <class Fn>
  void postToFollowers(const std::vector<Follower>& followers, Fn fn);


public:
  InFlight(std::weak_ptr<MyCache> cache,
           const std::string& key,
           TimeSource& time_source);

  ~InFlight() override;

  bool start(Upstream::ThreadLocalCluster& cluster,
             Http::RequestHeaderMapPtr headers);

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
    std::shared_ptr<Entry> entry_ = nullptr;

    bool cacheable_ = false;
    bool shareable_ = false;
    bool destroying_ = false;
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
