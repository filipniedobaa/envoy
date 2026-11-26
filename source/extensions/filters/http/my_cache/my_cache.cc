#include "source/extensions/filters/http/my_cache/my_cache.h"

#include <memory>
#include <string>

#include "envoy/common/time.h"
#include "envoy/event/dispatcher.h"
#include "envoy/http/header_map.h"

#include "source/common/http/header_map_impl.h"
#include "source/extensions/filters/http/my_cache/in_flight.h"

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {

void SharedBody::append(std::string chunk) {
  absl::MutexLock lock(&mutex_);
  bytes_ += chunk.size();
  chunks_.push_back(std::make_shared<const std::string>(std::move(chunk)));
}

void SharedBody::markComplete() {
  absl::MutexLock lock(&mutex_);
  complete_ = true;
}

SharedBody::Read SharedBody::readFrom(size_t index) const {
  absl::MutexLock lock(&mutex_);
  SharedBody::Read read;
  if (index < chunks_.size())
    read.chunks.assign(chunks_.begin() + index, chunks_.end());

  read.complete = complete_;
  if (complete_ && trailers_ != nullptr)
    read.trailers = Http::createHeaderMap<Http::ResponseTrailerMapImpl>(*trailers_);
  return read;
}

uint64_t SharedBody::size() const {
  absl::MutexLock lock(&mutex_);
  return bytes_;
}

void SharedBody::setTrailers(Http::ResponseTrailerMapPtr trailers) {
  absl::MutexLock lock(&mutex_);
  trailers_ = std::move(trailers);
}

// The whole decision happens under the locks, so among requests that go through
// the cache there is at most one InFlight per URL: a request either finds the
// stored entry, joins the fetch in progress, or creates it.
// Requests that bypass the cache (see requestMayCache) and clients a fetch
// rejects or releases (non-shareable response, push mode) go to the origin on
// their own, through the router.
LookupResult MyCache::lookupOrFollow(const std::string& key, MonotonicTime now,
                                     std::weak_ptr<MyCacheFilter> filter,
                                     Event::Dispatcher* dispatcher) {
  absl::MutexLock lock(&cache_mutex_);

  LookupResult res;

  auto it = map_.find(key);
  if (it != map_.end()) {
    Slot& slot = it->second;
    if (slot.entry->born_at + slot.entry->life_time <= now) {
      // IS CACHED BUT EXPIRED
      deleteFromCache(it);
    } else {
      // IS CACHED AND VALID
      lru_.splice(lru_.begin(), lru_, slot.lru_it);

      res.kind = LookupResult::Kind::Hit;
      res.entry = slot.entry;
      return res;
    }
  }

  absl::MutexLock in_flight_lock(&in_flight_mutex_);
  auto in_flight_record = in_flight_map_.find(key);
  if (in_flight_record == in_flight_map_.end()) {
    // WE ARE THE LEADER

    res.kind = LookupResult::Kind::Leader;
    res.in_flight = std::make_shared<InFlight>(weak_from_this(), key, dispatcher->timeSource(),
                                               max_object_bytes_);
    res.in_flight->addFollower(filter, dispatcher);

    in_flight_map_.emplace(key, res.in_flight);
    return res;
  }

  // WE ARE THE FOLLOWER
  if (!in_flight_record->second->addFollower(filter, dispatcher)) {
    res.kind = LookupResult::Kind::NonShareable;
    return res;
  }
  res.in_flight = in_flight_record->second;
  res.kind = LookupResult::Kind::Follower;
  return res;
}

void MyCache::insert(const std::string& key, EntrySharedPtr entry) {
  uint64_t bytes = entry->body->size() + entry->headers->byteSize() + key.size();
  if (bytes > max_object_bytes_)
    return;

  absl::MutexLock lock(&cache_mutex_);
  auto it = map_.find(key);
  if (it != map_.end())
    deleteFromCache(it);

  Slot slot;
  slot.bytes = bytes;
  slot.entry = std::move(entry);
  lru_.push_front(key);
  slot.lru_it = lru_.begin();
  map_.insert_or_assign(key, std::move(slot));
  total_bytes_ += bytes;
  removeLeastRecentlyUsed();
}

void MyCache::deleteInFlight(const std::string& key) {
  absl::MutexLock lock(&in_flight_mutex_);
  in_flight_map_.erase(key);
}

void MyCache::removeLeastRecentlyUsed() {
  cache_mutex_.AssertHeld();
  while (!lru_.empty() && total_bytes_ > max_total_bytes_) {
    deleteFromCache(lru_.back());
  }
}

void MyCache::deleteFromCache(const std::string& key) {
  // private so assume locked cache_mutex_
  cache_mutex_.AssertHeld();
  auto it = map_.find(key);
  if (it == map_.end())
    return;
  deleteFromCache(it);
}
void MyCache::deleteFromCache(absl::flat_hash_map<std::string, Slot>::iterator it) {
  // private so assume locked cache_mutex_
  cache_mutex_.AssertHeld();
  Slot& slot = it->second;
  lru_.erase(slot.lru_it);
  total_bytes_ -= slot.bytes;
  map_.erase(it);
}

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
