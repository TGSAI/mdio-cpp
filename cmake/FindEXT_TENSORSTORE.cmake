IF ( NOT TARGET tensorstore )
  set(Python3_FIND_STRATEGY VERSION)

 include(FetchContent)

 FetchContent_Declare(
  tensorstore
  GIT_REPOSITORY
  https://github.com/brian-michell/tensorstore.git
  GIT_TAG v0.1.63_latest
)

FetchContent_MakeAvailable(tensorstore)

# Provide Tensorstore include directories
set(TENSORSTORE_INCLUDE_DIRS ${tensorstore_SOURCE_DIR})

# Create an imported target
add_library(EXT_TENSORSTORE::tensorstore INTERFACE IMPORTED)
set_target_properties(EXT_TENSORSTORE::tensorstore PROPERTIES
  INTERFACE_INCLUDE_DIRECTORIES "${TENSORSTORE_INCLUDE_DIRS}"
  INTERFACE_LINK_LIBRARIES tensorstore::tensorstore
)

  MESSAGE(STATUS "Found Tensorstore library")
ENDIF()
