#include <string>
#include <memory>
#include "source/extensions/filters/http/my_cache/my_cache.h"
#include "source/extensions/filters/http/my_cache/in_flight.h"

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


class InFlight;
class MyCacheFilter;


LookupResult MyCache::lookup_or_follow(const std::string& key, 
                             MonotonicTime now,
                             std::weak_ptr<MyCacheFilter> filter,
                             Event::Dispatcher* dispatcher){
    absl::ReaderMutexLock lock(&cache_mutex_);

    LookupResult res;

    auto it = map_.find(key);
    if(it != map_.end()){
      if(it->second->born_at + it->second->life_time <= now) {
        //IS CACHED BUT EXPIRED
        //absl::MutexLock deleteLock(&cache_mutex_);
        //map_.erase(key);
      }
      else{
        //IS CACHED AND VALID
        res.kind = LookupResult::Kind::Hit;
        res.entry = it->second;
        return res;
      }
    }
    
    
    absl::MutexLock in_flight_lock(&in_flight_mutex_);
    auto in_flight_record = in_flight_map_.find(key);
    if(in_flight_record == in_flight_map_.end()){
      //WE ARE THE LEADER

      res.kind = LookupResult::Kind::Leader;
      res.in_flight = std::make_shared<InFlight>(weak_from_this(), key, dispatcher->timeSource());
      res.in_flight->addFollower(filter, dispatcher);

      in_flight_map_.emplace(key, res.in_flight);
      return res;
    }

    //WE ARE THE FOLLOWER
    if(!in_flight_record->second->addFollower(filter, dispatcher)){
      res.kind = LookupResult::Kind::NonShareable;
      return res;
    }
    res.in_flight = in_flight_record->second;
    res.kind = LookupResult::Kind::Follower;
    return res;
}

void MyCache::insert(const std::string& key, EntrySharedPtr entry){
    absl::MutexLock lock(&cache_mutex_);
    map_.insert_or_assign(key, std::move(entry)); //proc ne copy?
    
}
void MyCache::delete_in_flight(const std::string& key){
  absl::MutexLock lock(&in_flight_mutex_);
  in_flight_map_.erase(key);
}

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy

