IF ( NOT TARGET tensorstore )
  set(Python3_FIND_STRATEGY VERSION)

  include(FetchContent)
  FetchContent_Declare(
    tensorstore
    GIT_REPOSITORY
    https://github.com/brian-michell/tensorstore.git
    GIT_TAG v0.1.63_latest
  )
  # Additional FetchContent_Declare calls as needed...

  FetchContent_MakeAvailable(tensorstore)

  # Define a target that depends on TensorStore...

  MESSAGE(STATUS "Found Tensorstore library")
ENDIF()
