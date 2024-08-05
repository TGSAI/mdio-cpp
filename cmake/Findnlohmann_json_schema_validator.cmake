include(FetchContent)

# Check if nlohmann_json target is available
if (NOT TARGET nlohmann_json::nlohmann_json)
  message(FATAL_ERROR "nlohmann_json target not found. Ensure Tensorstore provides it.")
endif()

# Fetch nlohmann_json_schema_validator if not already defined
if (NOT TARGET nlohmann_json_schema_validator)
  FetchContent_Declare(
    nlohmann_json_schema_validator
    GIT_REPOSITORY https://github.com/pboettch/json-schema-validator.git
    GIT_TAG 2.2.0
  )

  if(NOT BUILD_VALIDATOR)
    set(JSON_VALIDATOR_INSTALL OFF CACHE BOOL "Disable json validator install" FORCE)
  endif()

  FetchContent_MakeAvailable(nlohmann_json_schema_validator)

  add_library(nlohmann_json_schema_validator::nlohmann_json_schema_validator ALIAS nlohmann_json_schema_validator)

  target_include_directories(nlohmann_json_schema_validator INTERFACE
    $<BUILD_INTERFACE:${nlohmann_json_schema_validator_SOURCE_DIR}/include>
  )

  target_link_libraries(nlohmann_json_schema_validator INTERFACE nlohmann_json::nlohmann_json)

  # Install the validator target
  install(TARGETS nlohmann_json_schema_validator
    EXPORT mdioTargets
  )

  message(STATUS "Found json schema validator library")
endif()
