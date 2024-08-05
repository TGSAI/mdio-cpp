# Contributor Guide

Thank you for your interest in improving this project.
This project is open-source under the [Apache 2.0 license] and
welcomes contributions in the form of bug reports, feature requests, and pull requests.

Here is a list of important resources for contributors:

- [Source Code]
- [Documentation] (Updated link pending)
- [Issue Tracker]
- [Code of Conduct]

[apache 2.0 license]: https://opensource.org/licenses/Apache-2.0
[source code]: https://github.com/TGSAI/mdio-cpp
[documentation]: https://mdio-python.readthedocs.io/
[issue tracker]: https://github.com/TGSAI/mdio-cpp/issues

## How to report a bug

Report bugs on the [Issue Tracker].

When filing an issue, make sure to answer these questions:

- Which operating system and compiler are you using?
- Which version of this project are you using?
- What did you do?
- What did you expect to see?
- What did you see instead?

The best way to get your bug fixed is to provide a test case,
and/or steps to reproduce the issue.

## How to request a feature

Request features on the [Issue Tracker].

## How to set up your development environment

### Requied tools
- CMake 3.24 or better
- A C++17 compiler
  - GCC 11 or better
  - Clang 14 or better
- ASM_NASM compiler
  - NASM version 2.15.05
- Python 3.9 or better

### Optional tools (Code quality control)
- clang-format version 18
- cpplint version 1.6.1

### Optional tools (Integration)
- Python module xarray version 2024.6.0 or better

### Alternative
Another alternative is to use a [Development Container] has been setup to provide
an environment with the required dependencies. This facilitates development on
different systems.

This should seamlessly enable development for users of [VS Code] on systems with docker installed.

### Known Issues:

- `cmake ..` may print "CMake Deprecation Warning at build/_deps/nlohmann_json_schema_validator-src/CMakeLists.txt:1".

## How to include MDIO

Fetch the library with development requirements:

```CMake
FetchContent_Declare(
  mdio
  GIT_REPOSITORY https://github.com/TGSAI/mdio-cpp.git
  GIT_TAG main
)
FetchContent_MakeAvailable(mdio)
```

Link the library against your source:
(Note that the `mdio_INTERNAL_DEPS` variable is required for the linker)

```CMake
target_link_libraries(my_executible PRIVATE mdio ${mdio_INTERNAL_DEPS})
```

You can now include **MDIO** in your source code:

```C++
#include <mdio/mdio.h>
```

[development container]: https://containers.dev/
[vs code]: https://code.visualstudio.com/docs/devcontainers/containers/

## How to test the project

```bash
$ mkdir build
$ cd build
# NOTE: "CMake Deprecation Warning at build/_deps/nlohmann_json_schema_validator-src/CMakeLists.txt:1" can safely be ignored
$ cmake ..
```
Each **MDIO** target has the prefix "mdio" in its name, to build the tests run the following commands from the build directory:
```bash
$ make -j32 mdio_acceptance_test
```
The acceptance test will validate that the MDIO/C++ data can be read by Python's Xarray. To ensure that the test passes, make sure your Python environment has Xarray install, and run the acceptance test:
```bash
$ cd build/mdio/
$ ./mdio_acceptance_test
```
The dataset and variables have their own test suite too: 
```bash
$ make -j32 mdio_variable_test
$ make -j32 mdio_dataset_test
```
Each **MDIO** library will provide an associated cmake alias, e.g. mdio::mdio which can be use to link against **MDIO** in your project.

## How to submit changes

Open a [pull request] to submit changes to this project.

Your pull request needs to meet the following guidelines for acceptance:

- The unit test suite must pass without errors and warnings.
- The format and linting pass without errors or warnings.
  - Using `// NOLINT` or `// clang-format off` should be used sparingly and narrowly.
- Include unit tests. This project aims to maintain 100% code coverage.
- If your changes add functionality, update the documentation accordingly.

Feel free to submit early, though—we can always iterate on this. Drafts are encouraged.

It is recommended to open an issue before starting work on anything.
This will allow a chance to talk it over with the owners and validate your approach.

[pull request]: https://github.com/TGSAI/mdio-cpp/pulls

<!-- github-only -->

[code of conduct]: CODE_OF_CONDUCT.md