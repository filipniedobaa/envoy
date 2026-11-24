#pragma once

#include <string>
#include <list>
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

constexpr uint64_t kMaxTotalBytes  = 256 * 1024 * 1024;  // 256 MiB
constexpr uint64_t kMaxObjectBytes =  32 * 1024 * 1024;  //  32 MiB

struct Entry{
  MonotonicTime born_at;
  std::chrono::seconds life_time;
  Http::ResponseHeaderMapPtr headers;
  std::string body;
};
using EntrySharedPtr = std::shared_ptr<const Entry>;

struct Slot{
  EntrySharedPtr entry;
  std::list<std::string>::iterator lru_it;
  uint64_t bytes;
};

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
    void remove_least_recently_used() ABSL_EXCLUSIVE_LOCKS_REQUIRED(cache_mutex_);
    void delete_from_cache(const std::string& key) ABSL_EXCLUSIVE_LOCKS_REQUIRED(cache_mutex_);
    void delete_from_cache(absl::flat_hash_map<std::string,Slot>::iterator it) ABSL_EXCLUSIVE_LOCKS_REQUIRED(cache_mutex_);
    absl::Mutex cache_mutex_;
    absl::Mutex in_flight_mutex_;
    std::list<std::string> lru_ ABSL_GUARDED_BY(cache_mutex_);
    absl::flat_hash_map<std::string, Slot> map_ ABSL_GUARDED_BY(cache_mutex_);
    absl::flat_hash_map<std::string, InFlightSharedPtr> in_flight_map_ ABSL_GUARDED_BY(in_flight_mutex_);
    uint64_t total_bytes_ ABSL_GUARDED_BY(cache_mutex_) = 0;

};
using MyCacheSharedPtr = std::shared_ptr<MyCache>;

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
