# MDIO v1.1

Welcome to the MDIO - a descriptive format for energy data that is intended to reduce storage costs,  improve the efficiency of I/O and make energy data and workflows understandable and reproducible.

MDIO schema definitions [here.](https://mdio-python.readthedocs.io/en/v1-new-schema/data_models/version_1.html)

# Getting Started

First clone the MDIO v1.1 library:

This project uses CMake for the build and requires CMake 3.24 or better to build. The project build is configured to use the fetch and install it 3rd party dependencies. To build the mdio, clone the repos and create a build directory:
```
mkdir build
cd build
cmake ..
```
Each mdio target has the preffix "mdio" in its name, to build the tests run the following commands from the build directory (compile twice to handle CMAKE issue with validator dependency):
```
make -j32 mdio_acceptance_test
# FIXME - ::nlohmann::json::validator CMAKE issue, re-run build
make -j32 mdio_acceptance_test 
```
The acceptance test will validate that the MDIO/C++ data can be read by Python's Xarray. To ensure that the test passes, make sure your Python environment has Xarray install, and run the acceptance test:
```
cd build/mdio/
./mdio_acceptance_test
```
The dataset and variables have their own test suite too: 
```
make -j32 mdio_variable_test
make -j32 mdio_dataset_test
```
Each mdio library will provide an associated cmake alias, e.g. mdio::mdio which can be use to link against mdio in your project.

## API Documentation

MDIO API documentation is currently provided with the MDIO library.
```
open mdio/docs/html/index.html
```

## Key Features

- **Standardized Schema Compliance**: MDIO enforces a strict adherence to a standardized schema for all data inputs, ensuring consistency, reliability, and ease of data interoperability.

- **Cloud and On-Premise Storage**: MDIO is intended to efficiently support energy datasets for local filesystems and HPC, and cloud object stores. Currenlty MDIO supports gcs and s3.

- **Xarray and Python mdio Compatibility**: We prioritize compatibility with popular data analysis tools like Xarray and Python mdio, allowing for straightforward integration with your existing workflows.

- **High Scalability and Performance**: Scalable asynchronous and concurrent I/O and tensor operations to handle complex and large energy datasets with ease, ensuring that your data processing remains fast and efficient, even as your data grows.

## Project Vision

Our vision is to provide a tool that not only simplifies the management of energy data but also enhances the quality and depth of energy analysis. By keeping units, dimensions, and other critical metadata with the data, MDIO ensures that every dataset is not just a collection of numbers but a rich, self-explaining narrative of energy insights.

## Target Audience

MDIO is built for a wide range of users, including:

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
  - <span style="color:green">✔</span> Implement benchmarking to identify performance bottlenecks.
  - Optimize high-impact areas identified through benchmarking.

#### Phase 3: Cost Reduction and Efficiency
- **Goal**: Reduce operational costs and improve efficiency.
- **Milestones**:
  - Build dataset factory methods for common use cases.
  - Streamline data management processes to reduce storage or compute costs (e.g. "MDIO v0.1", "SEGY", "SEP" I/O).

#### Phase 4: Feature Completeness and Compliance
- **Goal**: Ensure feature completeness and compliance with standards.
- **Milestones**:
  - Complete implementation of deferred features (e.g., "numpy slice semantics").

#### Phase 5: Process Optimization
- **Milestones**:
  - Analyze how clients are using MDIO and articulate bottlenecks and pain points.
  - Improve documentation and examples to reduce the cognitive load of adopting MDIO.
  - Resolve performance critical issues (runtime or storage costs).

## (dependency) Google's Tensorstore library

We use the [tensorstore](https://google.github.io/tensorstore/) library to provide native a C/C++ interface to 
ZArr. If you're familiar with the Python DASK library, tensorstore has very similar semantics when it 
comes to manipulating data and creating asynchronous execution.

tensorstore is used under an Apache 2.0 license.

Relevant features of the tensorstore library are:

1. Read/write ZArr data in memory, from disk, with GCFS buckets (Google file system).
2. Encode/decode data with some basic data compression BLOCS, zlib, lz4, zstd and jpeg.
3. Concurrency; concurrent (multi-threaded) reads/writes.
4. Objects designed with async futures/promises architecture.
4. Logical array slicing operations.
5. Basic iterators.
6. Chunk aligned iterators.
7. Informative error messages and exception handling.

Nice to have features of tensorstore:

1. A **companion** Python library.
2. Transactions, used to stage groups of modifications.
3. Caching.
4. Progress monitoring.
5. Abstraction over the tensorstore "driver", read generic array data from buckets.  

# Getting Started

This project uses CMake for the build and requires CMake 3.24 or better to build. 
The project CMakeLists.txt is configured to use the FetchContent to install the Tensorstore dependencies
automatically.

To build the mdio tests clone the repos and create a build directory:
```
mkdir build
cd build
cmake ..
```

Each mdio target has the word "mdio" appended to it, to build the tests run 
the following commands from the build directory:
```
make -j32 mdio_mdio_test
make -j32 mdio_mdio_data_test
```

These tests will be installed here:
```
build/mdio/mdio_mdio_test
build/mdio/mdio_mdio_data_test
```
and can be run from the command-line.

Each mdio library will provide an associated CMake alias, e.g. mdio::mdio
which can be use to link against mdio in a large project.

## Authors
- **Ben Lasscock** - *Initial Work* - [blasscoc](https://github.com/blasscoc)
- **Brian Michell** - *Initial Work* - [BrianMichell](https://github.com/BrianMichell)






