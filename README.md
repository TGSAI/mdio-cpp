<div>
  <img
      class="logo"
      src="https://raw.githubusercontent.com/TGSAI/mdio.github.io/gh-pages/assets/images/mdio.png"
      alt="MDIO"
      width=200
      height=auto
      style="margin-top:10px;margin-bottom:10px"
  />
</div>

[![License][license-image]][license-url]

[![C/C++ build](https://github.com/TGSAI/mdio-cpp/actions/workflows/cmake_build.yaml/badge.svg)](https://github.com/TGSAI/mdio-cpp/actions/workflows/cmake_build.yaml)
[![clang-format check](https://github.com/TGSAI/mdio-cpp/actions/workflows/clang-format-check.yml/badge.svg)](https://github.com/TGSAI/mdio-cpp/actions/workflows/clang-format-check.yml)
[![CodeQL Analysis](https://github.com/TGSAI/mdio-cpp/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/TGSAI/mdio-cpp/actions/workflows/codeql-analysis.yml)


Welcome to **MDIO** - a descriptive format for energy data that is intended to reduce storage costs,  improve the efficiency of I/O and make energy data and workflows understandable and reproducible.

**MDIO** schema definitions [here.](https://mdio-python.readthedocs.io/en/v1-new-schema/data_models/version_1.html)

# Requied tools
- CMake 3.24 *or better*
- A C++17 compiler
  - GCC 11 *or better*
  - Clang 14 *or better*
- ASM_NASM compiler
  - NASM version 2.15.05
- Python 3.9 *or better*

## Optional tools (Code quality control)
- clang-format version 18
- cpplint version 1.6.1

## Optional tools (Integration)
- Python module xarray version 2024.6.0 *or better*

# Getting Started

First clone the **MDIO** v1.0 library:

This project uses CMake for the build and requires CMake 3.24 *or better* to build. The project build is configured to use the fetch and install it 3rd party dependencies. To build MDIO, clone the repos and create a build directory:
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

## API Documentation

**MDIO** API documentation is currently provided with the **MDIO** library.
```
open mdio/docs/html/index.html
```

## Key Features

- **Standardized Schema Compliance**: **MDIO** enforces a strict adherence to a standardized schema for all data inputs, ensuring consistency, reliability, and ease of data interoperability.

- **Cloud and On-Premise Storage**: **MDIO** is intended to efficiently support energy datasets for local filesystems and HPC, and cloud object stores. Currently **MDIO** supports cloud storage with GCS and S3.

- **Xarray and Python MDIO Compatibility**: We prioritize compatibility with popular data analysis tools like Xarray and Python **MDIO**, allowing for straightforward integration with your existing workflows.

- **High Scalability and Performance**: Scalable asynchronous and concurrent I/O and tensor operations to handle complex and large energy datasets with ease, ensuring that your data processing remains fast and efficient, even as your data grows.

## Project Vision

Our vision is to provide a tool that not only simplifies the management of energy data but also enhances the quality and depth of energy analysis. By keeping units, dimensions, and other critical metadata with the data, **MDIO** ensures that every dataset is not just a collection of numbers but a rich, self-explaining narrative of energy insights.

## Target Audience

**MDIO** is built for a wide range of users, including:

- **New Energy Solution**: WRF wind data models and associated 2-d attributes.

### Project Roadmap

#### Phase 1: Adoption, bug Fixes and stability
- **Goal**: Improve the reliability and stability of the tool.
- **Milestones**:
  - <span style="color:green">✔</span> Complete critical updates to test suites.
  - <span style="color:green">✔</span> Address issues causing undefined behavior or potential crashes.

#### Phase 2: I/O Performance Optimization
- **Goal**: Enhance the efficiency and performance of the tool.
- **Milestones**:
  - Optimize high-impact areas identified through benchmarking and real world usage.

#### Phase 3: Cost Reduction and Efficiency
- **Goal**: Reduce operational costs and improve efficiency.
- **Milestones**:
  - Build dataset factory methods for common use cases.
  - Streamline data management processes to reduce storage or compute costs (e.g. "**MDIO** v0.1", "SEGY", "SEP" I/O).

#### Phase 4: Feature Completeness and Compliance
- **Goal**: Ensure feature completeness and compliance with standards.
- **Milestones**:
  - Complete implementation of deferred features (e.g., "numpy slice semantics").

#### Phase 5: Process Optimization
- **Milestones**:
  - Analyze how clients are using **MDIO** and articulate bottlenecks and pain points.
  - Improve documentation and examples to reduce the cognitive load of adopting **MDIO**.
  - Resolve performance critical issues (runtime or storage costs).

## (dependency) Tensorstore

We use the [tensorstore](https://google.github.io/tensorstore/) library to provide native a C/C++ interface to 
ZArr. If you're familiar with the Python DASK library, tensorstore has very similar semantics when it 
comes to manipulating data and creating asynchronous execution.

Tensorstore is used under an Apache 2.0 license.

Relevant features of the Tensorstore library are:

1. Read/write ZArr data in memory, from disk, with GCFS buckets (Google file system).
2. Encode/decode data with some basic data compression BLOCS, zlib, lz4, zstd and jpeg.
3. Concurrency; multi-threaded ACID reads/writes.
4. Objects designed with async futures/promises architecture.
4. Logical array slicing operations.
5. Basic iterators.
6. Chunk aligned iterators.
7. Informative error messages and exception handling.

Nice to have features of Tensorstore:

1. A **companion** Python library.
2. Transactions, used to stage groups of modifications.
3. Caching.
4. Progress monitoring.
5. Abstraction over the Tensorstore "driver", read generic array data from buckets.  

## (dependency) Patrick Boettcher's JSON schema validator

We use the [json-schema-validator](https://github.com/pboettch/json-schema-validator) library to validate **MDIO** schemas against the [schema definitions](https://mdio-python.readthedocs.io/en/v1-new-schema/data_models/version_1.html).

This library is used under the [MIT](https://github.com/pboettch/json-schema-validator?tab=License-1-ov-file#readme) license.


## Authors
- **Ben Lasscock** - *Initial Work* - [blasscoc](https://github.com/blasscoc)
- **Brian Michell** - *Initial Work* - [BrianMichell](https://github.com/BrianMichell)



[license-image]: https://img.shields.io/badge/License-Apache%202.0-blue.svg
[license-url]: LICENSE
