#include "source/extensions/filters/http/my_cache/my_cache_filter.h"
#include "source/extensions/filters/http/my_cache/cacheability.h"
#include "source/extensions/filters/http/my_cache/in_flight.h"

#include "source/common/buffer/buffer_impl.h"
#include "source/common/http/header_map_impl.h"
#include "source/common/http/headers.h"

#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/strip.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace MyCacheFilter {

Http::FilterHeadersStatus MyCacheFilter::decodeHeaders(Http::RequestHeaderMap& headers,
                                                       bool) {
  if(headers.getMethodValue() != "GET") 
    return Http::FilterHeadersStatus::Continue;

  if(!requestMayCache(headers)) return Http::FilterHeadersStatus::Continue;

  std::string key = absl::StrCat(headers.getHostValue(), headers.getPathValue());

  LookupResult lookup_result =
      cache_->lookup_or_follow(key, 
                               decoder_callbacks_->dispatcher().timeSource().monotonicTime(),
                               weak_from_this(),
                               &decoder_callbacks_->dispatcher());
  if(lookup_result.kind != LookupResult::Kind::NonShareable){
    decoder_callbacks_->addDownstreamWatermarkCallbacks(*this);
    watermark_registered_ = true;
  }
  
  switch (lookup_result.kind)
  {
    case LookupResult::Kind::Hit:
    {
      auto headers = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(*lookup_result.entry->headers);
      headers->addCopy(Http::LowerCaseString("my-cache"), "HIT");

      std::chrono::seconds age = std::chrono::duration_cast<std::chrono::seconds>(
        decoder_callbacks_->dispatcher().timeSource().monotonicTime()
        - lookup_result.entry->born_at);

      headers->setCopy(Http::LowerCaseString("age"),
                       std::to_string(age.count()));

      decoder_callbacks_->encodeHeaders(std::move(headers), false, "HIT");

      body_ = lookup_result.entry->body;
      pull();
      
      return Http::FilterHeadersStatus::StopIteration;
    }

    case LookupResult::Kind::Leader:
    {
      Upstream::ThreadLocalCluster* cluster = cluster_manager_.getThreadLocalCluster("origin");
      if(cluster == nullptr
      || !lookup_result.in_flight->start(*cluster, Http::createHeaderMap<Http::RequestHeaderMapImpl>(headers))){
        
        lookup_result.in_flight->fail();
      }
      return Http::FilterHeadersStatus::StopIteration;
    }
    case LookupResult::Kind::Follower:
    {
      return Http::FilterHeadersStatus::StopIteration;
    }
    case LookupResult::Kind::NonShareable:
    {
      return Http::FilterHeadersStatus::Continue;
    }
  }
  return Http::FilterHeadersStatus::StopIteration; //OR PANIC
}

void MyCacheFilter::onUpstreamHeaders(Http::ResponseHeaderMapPtr headers, SharedBodyPtr body){
  if(destroyed_ || done_) return;
  body_ = body;
  decoder_callbacks_->encodeHeaders(std::move(headers), false, "my_cache_miss");
}

void MyCacheFilter::onUpstreamData(const std::string& chunk){
  //only for push mode
  std::shared_ptr<const std::string> tmp = std::make_shared<const std::string>(chunk);
  if(destroyed_ || done_) return;
  auto* fragment = new Buffer::BufferFragmentImpl(
    tmp->data(), chunk.size(),
    [tmp](const void*, size_t, const Buffer::BufferFragmentImpl* self){
      delete self;
    });

  Buffer::OwnedImpl buffer;
  buffer.addBufferFragment(*fragment);
  decoder_callbacks_->encodeData(buffer, false);
}

void MyCacheFilter::onBodyAvailable(){
  pull();
}

void MyCacheFilter::onTrailers(Http::ResponseTrailerMapPtr trailers){
  if(destroyed_ || done_) return;
  done_ = true;
  decoder_callbacks_->encodeTrailers(std::move(trailers));
}

void MyCacheFilter::onComplete(){
  if(destroyed_ || done_) return;
  done_ = true;
  Buffer::OwnedImpl empty_body("");
  decoder_callbacks_->encodeData(empty_body, true);
}
void MyCacheFilter::onUpstreamFail(){
  if(destroyed_ || done_) return;
  done_ = true;
  decoder_callbacks_->sendLocalReply(Http::Code::ServiceUnavailable,
                                       "upstream fetch failed",
                                       nullptr, std::nullopt,
                                       "upstream_fetch_failed");
}
void MyCacheFilter::onReset(){
  if(destroyed_ || done_) return;
  done_ = true;
  decoder_callbacks_->resetStream();
}

void MyCacheFilter::onDestroy(){ //called by dispatcher?
  destroyed_ = true;
  //because envoy call onDestroy() first and deletes the object later,
  //so callbacks would be invalid

  if (watermark_registered_) {
    decoder_callbacks_->removeDownstreamWatermarkCallbacks(*this);
    watermark_registered_ = false;
  }
}

void MyCacheFilter::onBypass(){
  if(destroyed_ || done_) return;
  done_ = true;

  if (watermark_registered_) {
    decoder_callbacks_->removeDownstreamWatermarkCallbacks(*this);
    watermark_registered_ = false;
  }
  
  decoder_callbacks_->continueDecoding();
}

void MyCacheFilter::pull(){
  if(destroyed_ || done_ || high_watermark_counter_ > 0) return;
  if(body_ == nullptr) return;

  SharedBody::Read read = body_->readFrom(cursor_);
  for(auto chunk : read.chunks){

    auto* fragment = new Buffer::BufferFragmentImpl(
      chunk->data(), chunk->size(),
      [chunk](const void*, size_t, const Buffer::BufferFragmentImpl* self){
        delete self;
      });

    Buffer::OwnedImpl buffer;
    buffer.addBufferFragment(*fragment);
    decoder_callbacks_->encodeData(buffer, false);
    cursor_++;
    if(destroyed_ || done_ || high_watermark_counter_ > 0) return;
  }

  if(read.complete){
    done_ = true;
    if(read.trailers != nullptr){
      decoder_callbacks_->encodeTrailers(std::move(read.trailers));
    }
    else{
      Buffer::OwnedImpl empty_body("");
      decoder_callbacks_->encodeData(empty_body, true);
    }
  }
}

void MyCacheFilter::onAboveWriteBufferHighWatermark() {
  high_watermark_counter_++;
}

void MyCacheFilter::onBelowWriteBufferLowWatermark() {
  if (high_watermark_counter_ > 0) high_watermark_counter_--;
  if (high_watermark_counter_ == 0) {
    
    //cant call pull() directly because of stack re-entrancy????
    decoder_callbacks_->dispatcher().post([weak = weak_from_this()]() {
      if (auto self = weak.lock()) self->pull();
    });
  }
}


} // namespace MyCacheFilter
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
