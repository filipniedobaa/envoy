#include <string>
#include <memory>
#include "source/extensions/filters/http/my_cache/my_cache.h"

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "envoy/common/time.h"
#include "envoy/http/header_map.h"
#include "source/common/http/header_map_impl.h"


namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {


EntrySharedPtr MyCache::lookup(const std::string& key, MonotonicTime now){
    absl::ReaderMutexLock lock(&mutex_);
    auto it = map_.find(key);
    if(it == map_.end() || it->second->expires_at <= now) return nullptr;
    return it->second;
}

void MyCache::insert(const std::string& key, EntrySharedPtr entry){
    absl::MutexLock lock(&mutex_);
    map_.insert_or_assign(key, std::move(entry)); //proc ne copy?
    
}

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy

