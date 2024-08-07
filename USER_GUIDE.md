# MDIO User Guide
The goal of this user guide is to provide an introduction on how you may want to use **MDIO** in your own applications.

## Getting started
This user guide will assume that you are working in either the provided [devcontainer](https://github.com/TGSAI/mdio-cpp/.devcontainer) or have your environment configured according to the [README](https://github.com/TGSAI/mdio-cpp/README.md). Please ensure you have the [required tools](https://github.com/TGSAI/mdio-cpp?tab=readme-ov-file#requied-tools) before proceeding.

This user guide uses minimal BASH scripting in examples. Commands may need to be altered depending on your operating system.

## How to include MDIO
**MDIO** currently only supports the [CMake](https://cmake.org/) build system. This makes including **MDIO** straightforward in pre-existing CMake projects.

1. Ensure you have the `FetchContent` module included in your CMake project.
    ```Cmake
    include(FetchContent)
    ```
2. Select a version of **MDIO**. Main can be expected to be stable but may update unexpectedly. [Tagged](https://github.com/TGSAI/mdio-cpp/tags) versions will also be available if a specific version is desired.
    ```Cmake
    FetchContent_Declare(
      mdio
      GIT_REPOSITORY https://github.com/TGSAI/mdio-cpp.git
      GIT_TAG main
    )
    ```
3. Make the **MDIO** library available to use in your CMake project.
    ```Cmake
    FetchContent_MakeAvailable(mdio)
    ```
4. Link **MDIO** and its internal dependencies. By default, CMake will not properly link all internal dependencies, so we provide some helpful variables to take the guesswork out of this process. Required is the `mdio_INTERNAL_DEPS` variable. See the below section on linking for more details.
    ```Cmake
    target_link_libraries(my_program PRIVATE 
      mdio
      ${mdio_INTERNAL_DEPS}
    )
    ```

### What a full CMake might look like for Hello, World!
```Cmake
cmake_minimum_required(VERSION 3.24)
project(hello_world_project VERSION 1.0.0 LANGUAGES CXX)

# Set the C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include FetchContent module
include(FetchContent)

# Fetch the mdio-cpp library from the specified branch
FetchContent_Declare(
  mdio
  GIT_REPOSITORY https://github.com/TGSAI/mdio-cpp.git
  GIT_TAG main
)
FetchContent_MakeAvailable(mdio)

# Create an executable target
add_executable(hello_mdio src/hello_mdio.cc)

# Link the mdio library to the executable
target_link_libraries(hello_mdio PRIVATE 
  mdio
  ${mdio_INTERNAL_DEPS}
)
```

## Linking
As mentioned above, linking is not as straight forward as fetching the library and linking it against your binary. We provide a handful of linkable variables that streamline the process.
- `mdio_INTERNAL_DEPS` is required and should come immediately after `mdio` in the linking process
- `mdio_INTERNAL_GCS_DRIVER_DEPS` is only required if using [Google Cloud Store](https://cloud.google.com/storage). It should come after `mdio_INTERNAL_DEPS`.
- `mdio_INTERNAL_S3_DRIVER_DEPS` is only required if using [Amazon S3](https://aws.amazon.com/s3/). It should come after `mdio_INTERNAL_DEPS`.

It is worth noting that both the GCS drivers and S3 drivers can be linked at the same time and order does not matter. It is also notable that the order of inclusion *should not* strictly matter for CMake projects, but maintaining this order will help in diagnosing any issues you may run into.

## How to compile
1. In your root directory you should make a new directory called `build`.
    ```BASH
    $ mkdir build
    ```
2. Move into the `build` directory.
    ```BASH
    $ cd build
    ```
3. Run CMake. This may take up to several minutes depending on your system.
    ```BASH
    $ cmake ..
    ```
4. Run make. This may take up to several minutes depending on your system the first time compiling. Replace `my_program` with the name of your executable.
    ```BASH
    $ make -j$(nproc) my_program
    ```
5. Run your binary!
    ```BASH
    ./my_program
    ```

### What a full compile might look like for Hello, World!
```BASH
$ cd ~/mdio-cpp/examples/hello_mdio
$ pwd
# /home/BrianMichell/mdio-cpp/examples/hello_mdio
$ mkdir build
$ cd build
$ pwd
# /home/BrianMichell/mdio-cpp/examples/hello_mdio/build
$ cmake ..
$ make -j$(nproc) hello_mdio
$ ./hello_mdio
```

## Concepts


## Constructors


## Slicing
Slicing in **MDIO** is the concept of getting a subset of the data. This could be anything from a single point to a full [hypercube](https://en.wikipedia.org/wiki/Hypercube). Slicing is non-destructive, meaning that if you have pre-existing data that gets sliced that original data will remain untouched.

Currently **MDIO**-cpp only permits single step slicing to ensure data integrity.

Slicing in **MDIO**-cpp is handled through an `mdio::SliceDescriptor`, which requires 4 inputs per dimension. Those input are:

- Label: The name of the dimension to slice.
- Start: The first index. This is <u><b>inclusive</b></u>.
- Stop: The last index. This is <u><b>exclusive</b></u>.
- Step: The stride from one index to the next. This <u><b>must</b></u> be 1.

Below is an example of how we can construct a slice descriptor for the *X* dimension.
```cpp
mdio::SliceDescriptor xSlice = {
    /*label=*/ "X",
    /*start=*/ 0,
    /*stop= */ 100,
    /*step= */ 1
};
```

## Read
Reading **MDIO** is performed lazily, which means that we will only read a small amount of metadata from disk when the **MDIO** is opened. 

## Write

