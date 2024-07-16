IF ( NOT TARGET nlohmann_json_schema_validator )
  set(Python3_FIND_STRATEGY VERSION)

  include(FetchContent)
  FetchContent_Declare(
    nlohmann_json_schema_validator
    GIT_REPOSITORY
    https://github.com/pboettch/json-schema-validator.git
    GIT_TAG 2.2.0
  )

  # Disable the "install" because we will build the _deps as required
  # BUILD_VALIDATOR is a flag that gets set by `build_mdio.sh` for HPC builds
  
  if(NOT BUILD_VALIDATOR)
    set(JSON_VALIDATOR_INSTALL OFF CACHE BOOL "Disable json validator install" FORCE)
  endif()

  FetchContent_MakeAvailable(nlohmann_json_schema_validator)

  MESSAGE(STATUS "Found json schema validator library")
ENDIF()
