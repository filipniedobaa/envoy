#pragma once

#include <string>
#include <memory>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "envoy/common/time.h"
#include "envoy/http/header_map.h"
#include "source/common/http/header_map_impl.h"
#include "envoy/event/dispatcher.h"


namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {

struct Entry{
  MonotonicTime born_at;
  std::chrono::seconds life_time;
  Http::ResponseHeaderMapPtr headers;
  std::string body;
};
using EntrySharedPtr = std::shared_ptr<const Entry>;

class InFlight;
class MyCacheFilter;
using InFlightSharedPtr = std::shared_ptr<InFlight>;

struct LookupResult {
  enum class Kind { Hit, Follower, Leader, NonShareable };
  Kind kind;
  EntrySharedPtr entry = nullptr;
  InFlightSharedPtr in_flight = nullptr;
};




class MyCache : public std::enable_shared_from_this<MyCache>{
public:
    LookupResult lookup_or_follow(const std::string& key, 
                        MonotonicTime now,
                        std::weak_ptr<MyCacheFilter> follower,
                        Event::Dispatcher* dispatcher);
    void insert(const std::string& key, EntrySharedPtr entry);
    void delete_in_flight(const std::string& key);

private:
    absl::Mutex cache_mutex_;
    absl::Mutex in_flight_mutex_;
    absl::flat_hash_map<std::string, EntrySharedPtr> map_ ABSL_GUARDED_BY(cache_mutex_);
    absl::flat_hash_map<std::string, InFlightSharedPtr> in_flight_map_ ABSL_GUARDED_BY(in_flight_mutex_);

};
using MyCacheSharedPtr = std::shared_ptr<MyCache>;

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
