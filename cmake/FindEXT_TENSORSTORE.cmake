IF ( NOT TARGET tensorstore )
  set(Python3_FIND_STRATEGY VERSION)

 include(FetchContent)

FetchContent_Declare(
 tensorstore
 GIT_REPOSITORY
 https://github.com/google/tensorstore.git
 GIT_TAG 917edaf341217f750b7bd3b8db6e75e6db64eab8
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
