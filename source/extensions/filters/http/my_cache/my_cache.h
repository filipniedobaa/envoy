// RAM cache for the my_cache filter: stored responses (LRU, size-limited) plus a
// registry of origin fetches in progress, so concurrent misses for the same URL
// share one fetch (request coalescing).

#pragma once

#include <list>
#include <memory>
#include <string>

#include "envoy/common/time.h"
#include "envoy/event/dispatcher.h"
#include "envoy/http/header_map.h"

#include "source/common/http/header_map_impl.h"

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {

// The body of one response, stored once and shared by everyone who needs it:
// the fetch in progress (InFlight), the cached Entry, and every client still
// receiving it (MyCacheFilter).
// Append-only list of immutable chunks. Readers copy only the chunk pointers
// under the lock and read the bytes without it. Chunks are shared_ptrs, so a
// chunk stays alive while anyone uses it, including zero-copy fragments still
// queued in Envoy, even after the entry was evicted.
// Threads: written only on the leader's worker, read from any worker.
// mutex_ is a leaf lock: nothing else is called or locked while it is held.
class SharedBody {
public:
  void append(std::string chunk); // called by InFlight::onData (leader's worker)
  void markComplete();            // called by InFlight::onComplete
  struct Read {
    std::vector<std::shared_ptr<const std::string>> chunks; // chunks from `index` on
    bool complete;
    Http::ResponseTrailerMapPtr trailers;
  };
  void setTrailers(Http::ResponseTrailerMapPtr trailers);
  Read readFrom(size_t index) const; // called by MyCacheFilter::pull (any worker)
  uint64_t size() const;             // total bytes, for the size limits

private:
  mutable absl::Mutex mutex_; // `mutable`, so const readFrom/size can still lock it
  std::vector<std::shared_ptr<const std::string>> chunks_ ABSL_GUARDED_BY(mutex_);
  Http::ResponseTrailerMapPtr trailers_ ABSL_GUARDED_BY(mutex_) = nullptr;
  bool complete_ ABSL_GUARDED_BY(mutex_) = false;
  uint64_t bytes_ ABSL_GUARDED_BY(mutex_) = 0;
};
using SharedBodyPtr = std::shared_ptr<SharedBody>;

// A cached response. Immutable once inserted (shared as shared_ptr<const Entry>).
// born_at = arrival time minus the origin's Age header, so both the freshness
// check and the Age header on hits are a single subtraction.
struct Entry {
  MonotonicTime born_at;
  std::chrono::seconds life_time;
  Http::ResponseHeaderMapPtr headers;
  SharedBodyPtr body;
};
using EntrySharedPtr = std::shared_ptr<const Entry>;

// Value struct for cache map_ with a pointer to the key in lru_ list
// and the objects' size
struct Slot {
  EntrySharedPtr entry;
  std::list<std::string>::iterator lru_it;
  uint64_t bytes;
};

class InFlight;
class MyCacheFilter;
using InFlightSharedPtr = std::shared_ptr<InFlight>;

// Return package of lookupOrFollow(...). In decodeHeaders() we decide
// whether to respond right away, create a new InFlight request or join
// an existing one based off the Kind.
struct LookupResult {
  enum class Kind { Hit, Follower, Leader, NonShareable };
  Kind kind;
  EntrySharedPtr entry = nullptr;
  InFlightSharedPtr in_flight = nullptr;
};

// One instance per filter config, shared by all worker threads.
// Two maps, each with its own mutex:
//   map_ + lru_     (cache_mutex_)     - stored entries in LRU order,
//                                        total_bytes_ <= max_total_bytes_
//   in_flight_map_  (in_flight_mutex_) - one InFlight per URL being fetched
// Lock order: cache_mutex_ -> in_flight_mutex_ -> InFlight::mutex_, never the
// reverse. InFlight therefore releases its own lock before calling insert()
// or deleteInFlight().
// Hits take cache_mutex_ exclusively, because they move the entry to the front
// of the LRU list. Measured: negligible at 4 workers.
class MyCache : public std::enable_shared_from_this<MyCache> {
public:
  MyCache(uint64_t max_total_bytes, uint64_t max_object_bytes)
      : max_total_bytes_(max_total_bytes), max_object_bytes_(max_object_bytes) {}

  uint64_t maxObjectBytes() const { return max_object_bytes_; }

  LookupResult lookupOrFollow(const std::string& key, MonotonicTime now,
                              std::weak_ptr<MyCacheFilter> follower, Event::Dispatcher* dispatcher);
  void insert(const std::string& key, EntrySharedPtr entry);
  void deleteInFlight(const std::string& key);

private:
  void removeLeastRecentlyUsed() ABSL_EXCLUSIVE_LOCKS_REQUIRED(cache_mutex_);
  void deleteFromCache(const std::string& key) ABSL_EXCLUSIVE_LOCKS_REQUIRED(cache_mutex_);
  void deleteFromCache(absl::flat_hash_map<std::string, Slot>::iterator it)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(cache_mutex_);

  const uint64_t max_total_bytes_;
  const uint64_t max_object_bytes_;
  absl::Mutex cache_mutex_;
  absl::Mutex in_flight_mutex_;
  std::list<std::string> lru_ ABSL_GUARDED_BY(cache_mutex_);
  absl::flat_hash_map<std::string, Slot> map_ ABSL_GUARDED_BY(cache_mutex_);
  absl::flat_hash_map<std::string, InFlightSharedPtr>
      in_flight_map_ ABSL_GUARDED_BY(in_flight_mutex_);
  uint64_t total_bytes_ ABSL_GUARDED_BY(cache_mutex_) = 0;
};
using MyCacheSharedPtr = std::shared_ptr<MyCache>;

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
