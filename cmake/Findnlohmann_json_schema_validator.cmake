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
    GIT_TAG 2.4.0
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
  # The validator sets PUBLIC_HEADER to a path relative to its own source tree.
  # We re-install the target only to add it to the mdioTargets export set; we must
  # NOT re-copy its public header (the validator installs it itself, to
  # include/nlohmann/). Clearing the property stops install(TARGETS) from resolving
  # the relative header path against this directory and failing.
  set_target_properties(nlohmann_json_schema_validator PROPERTIES PUBLIC_HEADER "")
  install(TARGETS nlohmann_json_schema_validator
    EXPORT mdioTargets
  )

  message(STATUS "Found json schema validator library")
endif()
