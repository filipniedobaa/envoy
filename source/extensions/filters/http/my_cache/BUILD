load(
    "//bazel:envoy_build_system.bzl",
    "envoy_cc_extension",
    "envoy_cc_library",
    "envoy_extension_package",
)

licenses(["notice"])  # Apache 2

envoy_extension_package()

envoy_cc_library(
    name = "my_cache_lib",
    srcs = ["my_cache.cc"],
    hdrs = ["my_cache.h"],
    deps = [
        "//envoy/common:time_interface",
        "//envoy/http:header_map_interface",
        "//source/common/http:header_map_lib",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/synchronization",
    ],
)

envoy_cc_library(
    name = "my_cache_filter_lib",
    srcs = ["my_cache_filter.cc"],
    hdrs = ["my_cache_filter.h"],
    deps = [
        ":my_cache_lib",
        "//envoy/http:filter_interface",
        "//source/common/buffer:buffer_lib",
        "//source/common/http:header_map_lib",
        "//source/common/http:headers_lib",
        "//source/extensions/filters/http/common:pass_through_filter_lib",
        "@abseil-cpp//absl/strings",
    ],
)

envoy_cc_extension(
    name = "config",
    srcs = ["config.cc"],
    hdrs = ["config.h"],
    deps = [
        ":my_cache_filter_lib",
        "//envoy/registry",
        "//source/extensions/filters/http/common:factory_base_lib",
        "@envoy_api//envoy/extensions/filters/http/my_cache/v3:pkg_cc_proto",
    ],
)