#pragma once

#include <string>
#include <memory>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "envoy/common/time.h"
#include "envoy/http/header_map.h"
#include "source/common/http/header_map_impl.h"


namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {


struct Entry{
    MonotonicTime expires_at;
    Http::ResponseHeaderMapPtr headers;
    std::string body;
};

using EntrySharedPtr = std::shared_ptr<const Entry>;


class MyCache{
public:
    EntrySharedPtr lookup(const std::string& key, MonotonicTime now);
    void insert(const std::string& key, EntrySharedPtr entry);

private:
    absl::Mutex mutex_;
    absl::flat_hash_map<std::string, EntrySharedPtr> map_ ABSL_GUARDED_BY(mutex_);

};
using MyCacheSharedPtr = std::shared_ptr<MyCache>;

} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
