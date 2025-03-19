cmake_minimum_required(VERSION 3.24)
project(SeismicReader VERSION 1.0.0 LANGUAGES CXX)

# Set the C++ standard to C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Specify MDIO installation directory (set by bootstrap.sh)
set(MDIO_INSTALL_DIR "${CMAKE_SOURCE_DIR}/inst")

# Add library directories
link_directories(
    ${MDIO_INSTALL_DIR}/lib
    ${MDIO_INSTALL_DIR}/lib/drivers
)

# CURL is built and installed by the MDIO installer; no need for separate find_package.

# Define MDIO linker flags - corrected format to match SCons configuration
set(MDIO_LINK_FLAGS
    "-Wl,-rpath,${MDIO_INSTALL_DIR}/lib,-rpath,${MDIO_INSTALL_DIR}/lib/drivers,--whole-archive,-L${MDIO_INSTALL_DIR}/lib,-L${MDIO_INSTALL_DIR}/lib/drivers,\
-lnlohmann_json_schema_validator,\
-ltensorstore_driver_zarr_bzip2_compressor,\
-ltensorstore_driver_zarr_driver,\
-ltensorstore_driver_zarr_spec,\
-ltensorstore_driver_zarr_zlib_compressor,\
-ltensorstore_driver_zarr_zstd_compressor,\
-ltensorstore_driver_zarr_blosc_compressor,\
-ltensorstore_kvstore_gcs_http,\
-ltensorstore_kvstore_gcs_gcs_resource,\
-ltensorstore_kvstore_gcs_validate,\
-ltensorstore_kvstore_gcs_http_gcs_resource,\
-ltensorstore_kvstore_s3,\
-ltensorstore_kvstore_s3_aws_credentials_resource,\
-ltensorstore_kvstore_s3_credentials_default_credential_provider,\
-ltensorstore_kvstore_s3_credentials_environment_credential_provider,\
-ltensorstore_kvstore_s3_credentials_file_credential_provider,\
-ltensorstore_kvstore_s3_credentials_ec2_credential_provider,\
-ltensorstore_kvstore_s3_s3_metadata,\
-ltensorstore_kvstore_s3_s3_resource,\
-ltensorstore_driver_json,\
-ltensorstore_internal_cache_cache_pool_resource,\
-ltensorstore_internal_data_copy_concurrency_resource,\
-ltensorstore_kvstore_file,\
-ltensorstore_internal_file_io_concurrency_resource,\
-ltensorstore_internal_cache_kvs_backed_chunk_cache,\
-labsl,\
-lblosc,\
-ltensorstore,\
-lre2,\
-lriegeli,\
-ltinyxml2_tinyxml2,\
-lcurl,\
-lopenssl,\
--no-whole-archive,\
-lpthread,\
-lm"
)

# Debug: Print out the MDIO_LINK_FLAGS
message(STATUS "MDIO_LINK_FLAGS: ${MDIO_LINK_FLAGS}")

# Create the executable target. (Assumes main.cpp exists in your project.)
add_executable(read main.cc)

# Append the linker flags to the target's link flags.
set_target_properties(read PROPERTIES LINK_FLAGS "${MDIO_LINK_FLAGS}")

# Add compile definitions
target_compile_definitions(read PRIVATE HAVE_MDIO MAX_NUM_SLICES=32)

# Add MDIO and third-party include directories for target 'read'
# Collect all immediate subdirectories from the MDIO include directory.
file(GLOB CHILD_DIRS LIST_DIRECTORIES true "${MDIO_INSTALL_DIR}/include/*")

# Also include the top-level include directory so that headers like "mdio/mdio.h" are found.
list(INSERT CHILD_DIRS 0 "${MDIO_INSTALL_DIR}/include")

# Remove any unwanted directories (for example, the gtest-src directory)
foreach(dir ${CHILD_DIRS})
    get_filename_component(basename ${dir} NAME)
    if(basename MATCHES "gtest-src")
        list(REMOVE_ITEM CHILD_DIRS "${dir}")
    endif()
endforeach()

# Append additional directories that the installer uses but might not be one-level deep.
list(APPEND CHILD_DIRS 
    "${MDIO_INSTALL_DIR}/include/nlohmann_json-src/include"
    "${MDIO_INSTALL_DIR}/include/half-src/include"
)

target_include_directories(read PRIVATE ${CHILD_DIRS})

# Debug: Print out the include directories for target 'read'
get_target_property(READ_INCLUDES read INCLUDE_DIRECTORIES)
message(STATUS "Target 'read' include directories: ${READ_INCLUDES}")
