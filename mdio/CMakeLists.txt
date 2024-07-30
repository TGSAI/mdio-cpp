FIND_PACKAGE(EXT_TENSORSTORE)
FIND_PACKAGE(nlohmann_json_schema_validator)

mdio_cc_test(
  NAME
    gcs_test
  SRCS
    gcs_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::kvstore_memory
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
    nlohmann_json_schema_validator
    tensorstore::kvstore_gcs
)

mdio_cc_test(
  NAME
    variable_collection_test
  SRCS
    variable_collection_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::kvstore_memory
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
)

mdio_cc_test(
  NAME
    dataset_validator_test
  SRCS
    dataset_validator_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::tensorstore
    nlohmann_json_schema_validator
)

mdio_cc_test(
  NAME
    dataset_factory_test
  SRCS
    dataset_factory_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
  GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::kvstore_memory
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
    nlohmann_json_schema_validator
)

mdio_cc_test(
  NAME
    acceptance_test
  SRCS
    acceptance_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::kvstore_memory
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
    nlohmann_json_schema_validator
)

mdio_cc_test(
  NAME
    dataset_test
  SRCS
    dataset_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::kvstore_memory
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
    nlohmann_json_schema_validator
)

mdio_cc_test(
  NAME
    variable_test
  SRCS
    variable_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::kvstore_memory
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
)

mdio_cc_test(
  NAME
    utils_test
  SRCS
    utils_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::kvstore_memory
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
    tensorstore::kvstore_gcs
    nlohmann_json_schema_validator
)

mdio_cc_test(
  NAME
    stats_test
  SRCS
    stats_test.cc
  COPTS
    ${mdio_DEFAULT_COPTS} 
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    GTest::gmock_main
    tensorstore::driver_array
    tensorstore::driver_zarr
    tensorstore::driver_json
    tensorstore::kvstore_file
    tensorstore::kvstore_memory
    tensorstore::tensorstore
    tensorstore::index_space_dim_expression
    tensorstore::index_space_index_transform
    tensorstore::util_status_testutil
    nlohmann_json_schema_validator
)

FILE(GLOB mdio_include_files "${CMAKE_CURRENT_SOURCE_DIR}/*.h")
INSTALL(FILES ${mdio_include_files} DESTINATION include/mdio)

mdio_cc_library(
  NAME
    variable
  HDRS
    variable.h
  COPTS
    ${mdio_DEFAULT_COPTS}
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
    absl::status
    absl::strings  
    tensorstore::tensorstore
)

# TODO(blasscoc): Decide what is required below.
# Build out all other dependencies as shared libraries (for HPC)
mdio_static_library(
  NAME
    tensorstore
  COPTStensorstore::driver_json
    ${mdio_DEFAULT_COPTS}
  LINKOPTS
    ${mdio_DEFAULT_LINKOPTS}
  DEPS
  tensorstore::array
  tensorstore::array_storage_statistics
  tensorstore::batch
  tensorstore::box
  tensorstore::chunk_layout
  tensorstore::codec_spec
  tensorstore::context
  tensorstore::contiguous_layout
  tensorstore::data_type
  tensorstore::driver
  tensorstore::driver_array
  tensorstore::driver_json
  tensorstore::driver_json_json_change_map
  tensorstore::driver_kvs_backed_chunk_driver
  tensorstore::driver_zarr_blosc_compressor
  tensorstore::driver_zarr_bzip2_compressor
  tensorstore::driver_zarr_compressor
  tensorstore::driver_zarr_driver
  tensorstore::driver_zarr_dtype
  tensorstore::driver_zarr_metadata
  tensorstore::driver_zarr_spec
  tensorstore::driver_zarr_zlib_compressor
  tensorstore::driver_zarr_zstd_compressor
  tensorstore::index_interval
  tensorstore::index_space_add_new_dims_op
  tensorstore::index_space_alignment
  tensorstore::index_space_diagonal_op
  tensorstore::index_space_dimension_identifier
  tensorstore::index_space_dimension_selection
  tensorstore::index_space_dimension_units
  tensorstore::index_space_index_array_slice_op
  tensorstore::index_space_index_transform
  tensorstore::index_space_index_vector_or_scalar
  tensorstore::index_space_interval_slice_op
  tensorstore::index_space_label_op
  tensorstore::index_space_mark_explicit_op
  tensorstore::index_space_output_index_method
  tensorstore::index_space_single_index_slice_op
  tensorstore::index_space_transform_broadcastable_array
  tensorstore::index_space_transformed_array
  tensorstore::index_space_translate_op
  tensorstore::index_space_transpose_op
  tensorstore::internal_async_write_array
  tensorstore::internal_box_difference
  tensorstore::internal_cache
  tensorstore::internal_cache_async_cache
  tensorstore::internal_cache_cache_pool_resource
  tensorstore::internal_cache_chunk_cache
  tensorstore::internal_cache_kvs_backed_cache
  tensorstore::internal_chunk_grid_specification
  tensorstore::internal_compression_blosc
  tensorstore::internal_compression_blosc_compressor
  tensorstore::internal_compression_bzip2_compressor
  tensorstore::internal_compression_json_specified_compressor
  tensorstore::internal_compression_zlib
  tensorstore::internal_compression_zlib_compressor
  tensorstore::internal_compression_zstd_compressor
  tensorstore::internal_concurrency_resource
  tensorstore::internal_container_intrusive_linked_list
  tensorstore::internal_container_intrusive_red_black_tree
  tensorstore::internal_cord_util
  tensorstore::internal_data_copy_concurrency_resource
  tensorstore::internal_data_type_endian_conversion
  tensorstore::internal_digest_sha256
  tensorstore::internal_dimension_labels
  tensorstore::internal_env
  tensorstore::internal_file_io_concurrency_resource
  tensorstore::internal_grid_chunk_key_ranges
  tensorstore::internal_grid_chunk_key_ranges_base10
  tensorstore::internal_grid_partition
  tensorstore::internal_grid_partition_impl
  tensorstore::internal_grid_storage_statistics
  tensorstore::internal_http
  tensorstore::internal_http_curl_factory
  tensorstore::internal_http_curl_handle
  tensorstore::internal_http_curl_transport
  tensorstore::internal_http_curl_wrappers
  tensorstore::internal_http_http_header
  tensorstore::internal_json
  tensorstore::internal_json_array
  tensorstore::internal_json_binding
  tensorstore::internal_json_binding_data_type
  tensorstore::internal_json_binding_staleness_bound
  tensorstore::internal_json_binding_unit
  tensorstore::internal_json_metadata_matching
  tensorstore::internal_json_pointer
  tensorstore::internal_json_registry
  tensorstore::internal_json_same
  tensorstore::internal_json_value_as
  tensorstore::internal_lock_collection
  tensorstore::internal_masked_array
  tensorstore::internal_metrics
  tensorstore::internal_metrics_collect
  tensorstore::internal_metrics_metadata
  tensorstore::internal_metrics_metric_impl
  tensorstore::internal_metrics_registry
  tensorstore::internal_nditerable
  tensorstore::internal_nditerable_array
  tensorstore::internal_nditerable_copy
  tensorstore::internal_nditerable_data_type_conversion
  tensorstore::internal_nditerable_elementwise_input_transform
  tensorstore::internal_nditerable_elementwise_output_transform
  tensorstore::internal_nditerable_transformed_array
  tensorstore::internal_nditerable_util
  tensorstore::internal_oauth2
  tensorstore::internal_oauth2_google_auth_provider
  tensorstore::internal_oauth2_google_service_account_auth_provider
  tensorstore::internal_oauth2_oauth2_auth_provider
  tensorstore::internal_oauth2_oauth_utils
  tensorstore::internal_open_mode_spec
  tensorstore::internal_os_error_code
  tensorstore::internal_os_file_lister
  tensorstore::internal_os_file_util
  tensorstore::internal_path
  tensorstore::internal_poly_storage
  tensorstore::internal_rate_limiter
  tensorstore::internal_rate_limiter_admission_queue
  tensorstore::internal_rate_limiter_scaling_rate_limiter
  tensorstore::internal_rate_limiter_token_bucket_rate_limiter
  tensorstore::internal_retry
  tensorstore::internal_riegeli_array_endian_codec
  tensorstore::internal_riegeli_delimited
  tensorstore::internal_riegeli_json_input
  tensorstore::internal_riegeli_json_output
  tensorstore::internal_storage_statistics
  tensorstore::internal_thread
  tensorstore::internal_thread_pool_impl
  tensorstore::internal_thread_schedule_at
  tensorstore::internal_thread_task_group_impl
  tensorstore::internal_thread_thread_pool
  tensorstore::internal_unaligned_data_type_functions
  tensorstore::internal_uri_utils
  tensorstore::internal_utf8
  tensorstore::kvstore
  tensorstore::kvstore_byte_range
  tensorstore::kvstore_file
  tensorstore::kvstore_file
  tensorstore::kvstore_file_util
  tensorstore::kvstore_gcs_gcs_resource
  tensorstore::kvstore_gcs_http
  tensorstore::kvstore_gcs_http_gcs_resource
  tensorstore::kvstore_generation
  tensorstore::kvstore_http_byte_range_util
  tensorstore::kvstore_key_range
  tensorstore::kvstore_memory
  tensorstore::kvstore_s3
  tensorstore::kvstore_s3_s3_endpoint
  tensorstore::kvstore_s3_s3_request_builder
  tensorstore::kvstore_s3_s3_resource
  tensorstore::kvstore_s3_validate
  tensorstore::open_mode
  tensorstore::progress
  tensorstore::rank
  tensorstore::resize_options
  tensorstore::schema
  tensorstore::serialization
  tensorstore::serialization_absl_time
  tensorstore::serialization_function
  tensorstore::serialization_json
  tensorstore::serialization_registry
  tensorstore::spec
  tensorstore::static_cast
  tensorstore::strided_layout
  tensorstore::tensorstore
  tensorstore::transaction
  tensorstore::util_constant_vector
  tensorstore::util_element_pointer
  tensorstore::util_future
  tensorstore::util_iterate
  tensorstore::util_quote_string
  tensorstore::util_status
  tensorstore::util_stop_token
  tensorstore::util_unit
  tensorstore::util_utf8_string
  tensorstore::virtual_chunked
  tensorstore::internal_log_verbose_flag
)
install(TARGETS tensorstore DESTINATION lib)  

MACRO (TARGET_LINK_LIBRARIES_WHOLE_ARCHIVE target)
  IF (WIN32)
    FOREACH (arg IN LISTS ARGN)
      SET_TARGET_PROPERTIES(
        ${target} PROPERTIES LINK_FLAGS "/WHOLEARCHIVE:${lib}"
      )
    ENDFOREACH ()
  ELSE ()
    SET(LINK_FLAGS "-Wl,--whole-archive")
    SET(UNDO_FLAGS "-Wl,--no-whole-archive")
    TARGET_LINK_LIBRARIES(${target} PRIVATE ${LINK_FLAGS} ${ARGN} ${UNDO_FLAGS})
  ENDIF ()
ENDMACRO ()

add_library( absl SHARED dummy.cc)
TARGET_LINK_LIBRARIES_WHOLE_ARCHIVE (absl
absl::bad_any_cast_impl
absl::bad_optional_access
absl::bad_variant_access
absl::base
absl::city
absl::civil_time
absl::cord
absl::cord_internal
absl::cordz_functions
absl::cordz_handle
absl::cordz_info
absl::crc32c
absl::crc_cord_state
absl::crc_cpu_detect
absl::crc_internal
absl::debugging_internal
absl::demangle_internal
absl::examine_stack
absl::exponential_biased
absl::failure_signal_handler
absl::flags
absl::flags_commandlineflag
absl::flags_commandlineflag_internal
absl::flags_config
absl::flags_internal
absl::flags_marshalling
absl::flags_parse
absl::flags_private_handle_accessor
absl::flags_program_name
absl::flags_reflection
absl::flags_usage
absl::flags_usage_internal
absl::graphcycles_internal
absl::hash
absl::hashtablez_sampler
absl::int128
absl::kernel_timeout_internal
absl::leak_check
absl::log_entry
absl::log_globals
absl::log_internal_check_op
absl::log_internal_conditions
absl::log_internal_format
absl::log_internal_globals
absl::log_internal_log_sink_set
absl::log_internal_message
absl::log_internal_nullguard
absl::log_internal_proto
absl::log_severity
absl::log_sink
absl::low_level_hash
absl::malloc_internal
absl::random_distributions
absl::random_internal_platform
absl::random_internal_pool_urbg
absl::random_internal_randen
absl::random_internal_randen_hwaes
absl::random_internal_randen_hwaes_impl
absl::random_internal_randen_slow
absl::random_internal_seed_material
absl::random_seed_gen_exception
absl::random_seed_sequences
absl::raw_hash_set
absl::raw_logging_internal
absl::spinlock_wait
absl::stacktrace
absl::status
absl::statusor
absl::str_format_internal
absl::strerror
absl::strings
absl::strings_internal
absl::symbolize
absl::synchronization
absl::throw_delegate
absl::time
absl::time_zone
)
install(TARGETS absl DESTINATION lib)  

add_library( bzip2 SHARED dummy.cc)
TARGET_LINK_LIBRARIES_WHOLE_ARCHIVE ( bzip2
BZip2::BZip2
)
install(TARGETS bzip2 DESTINATION lib)  

add_library( blosc SHARED dummy.cc)
TARGET_LINK_LIBRARIES_WHOLE_ARCHIVE ( blosc
Blosc::blosc
Blosc::shuffle
Blosc::shuffle_avx2
Blosc::shuffle_generic
Blosc::shuffle_sse2
)
install(TARGETS blosc DESTINATION lib)  

add_library (openssl SHARED dummy.cc)
TARGET_LINK_LIBRARIES_WHOLE_ARCHIVE ( openssl
OpenSSL::Crypto
OpenSSL::SSL
)
install(TARGETS openssl DESTINATION lib)

add_library( lz4 SHARED dummy.cc)
TARGET_LINK_LIBRARIES_WHOLE_ARCHIVE ( lz4
LZ4::LZ4
)
install(TARGETS lz4 DESTINATION lib)  

add_library( re2 SHARED dummy.cc)
TARGET_LINK_LIBRARIES_WHOLE_ARCHIVE ( re2
re2::re2
)
install(TARGETS re2 DESTINATION lib)  

add_library( riegeli SHARED dummy.cc)
TARGET_LINK_LIBRARIES_WHOLE_ARCHIVE ( riegeli
riegeli::base_assert
riegeli::base_background_cleaning
riegeli::base_buffer
riegeli::base_chain
riegeli::base_cord_utils
riegeli::base_memory_estimator
riegeli::base_object
riegeli::base_parallelism
riegeli::base_recycling_pool
riegeli::base_shared_buffer
riegeli::base_sized_shared_buffer
riegeli::base_status
riegeli::base_string_utils
riegeli::base_zeros
riegeli::bytes_buffer_options
riegeli::bytes_buffered_reader
riegeli::bytes_buffered_writer
riegeli::bytes_chain_reader
riegeli::bytes_cord_reader
riegeli::bytes_cord_writer
riegeli::bytes_copy_all
riegeli::bytes_pullable_reader
riegeli::bytes_read_all
riegeli::bytes_reader_and_writer
riegeli::bytes_limiting_reader
riegeli::bzip2_bzip2_error
riegeli::bzip2_bzip2_reader
riegeli::bzip2_bzip2_writer
riegeli::varint_varint_reading
riegeli::zlib_zlib_error
riegeli::zlib_zlib_reader
riegeli::zlib_zlib_writer
riegeli::zstd_zstd_dictionary
riegeli::zstd_zstd_reader
riegeli::zstd_zstd_writer
)
install(TARGETS riegeli DESTINATION lib)  

add_library( snappy SHARED dummy.cc)
TARGET_LINK_LIBRARIES_WHOLE_ARCHIVE ( snappy
Snappy::Snappy
)
install(TARGETS snappy DESTINATION lib)  

add_library( zlib SHARED dummy.cc)
TARGET_LINK_LIBRARIES_WHOLE_ARCHIVE ( zlib
ZLIB::ZLIB
)
install(TARGETS zlib DESTINATION lib)