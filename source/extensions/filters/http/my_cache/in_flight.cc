#include "envoy/http/async_client.h"
#include "source/extensions/filters/http/my_cache/my_cache.h"
#include "source/extensions/filters/http/my_cache/cacheability.h"
#include "envoy/upstream/thread_local_cluster.h"
#include "source/extensions/filters/http/my_cache/in_flight.h"
#include "source/common/http/header_map_impl.h"
#include "source/extensions/filters/http/my_cache/my_cache_filter.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {

//-------SOURCE: CLAUDE---------
template <class Fn>
  void InFlight::postToFollowers(const std::vector<Follower>& followers, Fn fn) {
    for (const Follower& follower : followers) {
      follower.dispatcher->post([weak = follower.filter, fn]() {
        if (auto filter = weak.lock()) fn(*filter);
      });
    }
  }


InFlight::InFlight(std::weak_ptr<MyCache> cache, 
                   const std::string& key,
                   TimeSource& time_source):
                   cache_(cache), key_(key), time_source_(time_source){}

InFlight::~InFlight(){
  destroying_ = true;
  if(stream_ != nullptr) stream_->reset();
}

void InFlight::addFollower(std::weak_ptr<MyCacheFilter> filter,
                           Event::Dispatcher* dispatcher){
  absl::MutexLock lock(&mutex_);
  followers_.push_back({filter, dispatcher});
  if(response_headers_ == nullptr) return;

  
  dispatcher->post([weak = filter, 
                    data = entry_->body,
                    copied_headers = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(*response_headers_)]
                    () mutable {
    if(auto strong = weak.lock()){
      strong->onHeaders(std::move(copied_headers), false);
      strong->onData(data, false);
    }
  });
}

bool InFlight::start(Upstream::ThreadLocalCluster& cluster,
                     Http::RequestHeaderMapPtr headers){
  Http::AsyncClient::StreamOptions stream_options;
  stream_options.setTimeout(std::chrono::milliseconds(30000));

  stream_ = cluster.httpAsyncClient().start(*this, stream_options);
  if(!stream_) return false;

  request_headers_ = std::move(headers);
  stream_->sendHeaders(*request_headers_, true);
  return true;
}

void InFlight::onHeaders(Http::ResponseHeaderMapPtr&& headers, bool){
  absl::MutexLock lock(&mutex_);

  state_ = State::HeadersArrived;
  response_headers_ = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(*headers);
  std::optional<std::chrono::seconds> ttl = cacheableFor(*headers);
  entry_ = std::make_shared<Entry>();
  entry_->headers = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(*headers);
  if(ttl != std::nullopt){
    cacheable_ = true;
    entry_->expires_at = time_source_.monotonicTime() + *ttl;
  }

  for(const Follower& follower : followers_){
    follower.dispatcher->post(
      [weak = follower.filter, 
       copied_headers = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(*headers)]()mutable {
        if(auto strong = weak.lock())
          strong->onHeaders(std::move(copied_headers), false);
       });
  }
}

void InFlight::onData(Buffer::Instance& data, bool){
  absl::MutexLock lock(&mutex_);
  std::string data_string = data.toString();
  entry_->body.append(data_string);

  postToFollowers(followers_, [data_string](MyCacheFilter& filter){
    filter.onData(data_string, false);
  });
}

void InFlight::onTrailers(Http::ResponseTrailerMapPtr&& trailers){
  absl::MutexLock lock(&mutex_);

  cacheable_ = false;

  for(const Follower& follower : followers_){
    follower.dispatcher->post(
      [weak = follower.filter, 
       copied_trailers = Http::createHeaderMap<Http::ResponseTrailerMapImpl>(*trailers)]()mutable {
        if(auto strong = weak.lock())
          strong->onTrailers(std::move(copied_trailers));
       });
  }
}

void InFlight::onComplete(){
  {
    absl::MutexLock lock(&mutex_);
    if(state_ == State::Done) return;
    state_ = State::Done;
  }

  auto self = shared_from_this();

  //ORDER MATTERS HERE
  if(cacheable_){
    if(auto cache_shared_ptr = cache_.lock())
      cache_shared_ptr->insert(key_, entry_);
  }

  if(auto cache_shared_ptr = cache_.lock())
    cache_shared_ptr->delete_in_flight(key_);

  
  absl::MutexLock lock(&mutex_);
  stream_ = nullptr;
  
  postToFollowers(followers_, [](MyCacheFilter& filter){
    filter.onComplete();
  });
}

void InFlight::onReset(){
  State prev_state = State::Waiting; //init only cause of Werror
  if(destroying_)return;
  {
    absl::MutexLock lock(&mutex_);
    if(state_ == State::Done) return;
    prev_state = state_;
    state_ = State::Done;
  }

  auto self = shared_from_this();
  
  if(auto cache_shared_ptr = cache_.lock())
    cache_shared_ptr->delete_in_flight(key_);

  absl::MutexLock lock(&mutex_);
  if(prev_state == State::Waiting){
    postToFollowers(followers_, [](MyCacheFilter& filter){
      filter.onFail();
    });
  }
  else{
    postToFollowers(followers_, [](MyCacheFilter& filter){
      filter.onReset();
    });
  }

  stream_ = nullptr;
  entry_ = nullptr;
}

void InFlight::fail(){
  {
    absl::MutexLock lock(&mutex_);
    if(state_ == State::Done) return;
    state_ = State::Done;
  }
  auto self = shared_from_this();

  if(auto cache_shared_ptr = cache_.lock())
    cache_shared_ptr->delete_in_flight(key_);
  
  absl::MutexLock lock(&mutex_);
  postToFollowers(followers_, [](MyCacheFilter& filter){
    filter.onFail();
  });
  
}


} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy