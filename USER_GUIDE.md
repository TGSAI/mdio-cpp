# MDIO User Guide
The goal of this user guide is to provide an introduction on how you may want to use **MDIO** in your own applications.

## Table of contents
- [Getting started](#getting-started)
- [How to include MDIO](#how-to-include-mdio)
  - [What a full CMake might look like for Hello, World!](#what-a-full-cmake-might-look-like-for-hello-world)
- [Linking](#linking)
- [How to compile](#how-to-compile)
  - [What a full compile might look like for Hello, World!](#what-a-full-compile-might-look-like-for-hello-world)
- [Concepts](#concepts)
  - [Result based returns](#result-based-returns)
  - [Open options](#open-options)
  - [Variable, VariableData, and Dataset](#variable-variabledata-and-dataset)
- [Constructors](#constructors)
- [Slicing](#slicing)
- [Read](#read)
- [Write](#write)

## Getting started
This user guide will assume that you are working in either the provided [devcontainer](https://github.com/TGSAI/mdio-cpp/.devcontainer) or have your environment configured according to the [README](https://github.com/TGSAI/mdio-cpp/README.md). Please ensure you have the [required tools](https://github.com/TGSAI/mdio-cpp?tab=readme-ov-file#requied-tools) before proceeding. Following these guidelines should ensure a stable and consistent experience and will allow the community to provide better support without any guesswork regarding your environment.

This user guide uses minimal BASH scripting in examples. Commands may need to be altered depending on your operating system.

## How to include MDIO
**MDIO** currently only supports the [CMake](https://cmake.org/) build system. This makes including **MDIO** straightforward in pre-existing modern CMake projects.

1. Ensure you have the `FetchContent` module included in your CMake project.
    ```Cmake
    include(FetchContent)
    ```
2. Select a version of **MDIO**. Main can be expected to be stable but may update unexpectedly. [Tagged](https://github.com/TGSAI/mdio-cpp/tags) versions will also be available if a specific version is desired. New versions will be tagged as significant improvements, utilities, or features are added to the API, as well as when underlying dependencies are updated.
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

# Fetch the mdio-cpp library from the specified tag
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
# nproc will return the number of processors on your system. This helps speed up the build.
$ make -j$(nproc) hello_mdio
$ ./hello_mdio
```

## Concepts
### Result based returns
**MDIO** aims to follow the Google style of [not throwing exceptions](https://google.github.io/styleguide/cppguide.html#Exceptions). Instead, we use result based returns wherever an error state could exist. A trivial example of this design pattern is a simple function that tries to divide two integers, and handles the case of divide-by-zero.

```C++
int divide(int numerator, int denominator) {
  if (denominator == 0) {
    throw std::invalid_argument("Denominator cannot be zero");
  }
  return numerator / denominator;
}

int main() {
  int result;
  try {
    result = divide(10, 0);  // Oops, dividing by zero!
  } catch (const std::invalid_argument& e) {
    std::cerr << "Caught exception: " << e.what() << std::endl;
    return 1;
  }
  std::cout << "10 / 0 = " << result << std::endl;
  return 0;
}
```
Now let's recreate this example with **MDIO** result based returns instead.
```C++
mdio::Result<int> divide(int numerator, int denominator) {
  if (denominator == 0) {
    return absl::InvalidArgumentError("Denominator cannot be zero") ;
  }
  return numerator / denominator;
}

int main() {
  auto result = divide(10, 0);  // Oops, still dividing by zero!
  if (!result.status().ok()) {
    std::cerr << "Got a not-ok result: " << result.status() << std::endl;
    return 1;
  }
  std::cout << "10 / 0 = " << result.value() << std::endl;
  return 0;
}
```

Result based returns have the benefit of streamlined error handling, but do slightly increase the verbosity of your code. Adapting to this design pattern may feel uncomfortable for some, but we have found it very robust and clean.

If you find a case where **MDIO** throws an exception, please submit a [bug report](https://github.com/TGSAI/mdio-cpp/issues).

### Open options
Open options control how we interact with files, and **MDIO** is no different.
- `mdio::constants::kOpen`: Opens an **MDIO** for reading and writing. This is only valid for existing MDIO files and will return an error status if it does not exist.
- `mdio::constants::kCreate`: Opens a new **MDIO** for writing. This will return an error if the file already exists.
- `mdio::constants::kCreateClean`: Opens a new **MDIO** for writing. This <b><u>will</u></b> overwrite existing metadata and should only be used in testing. Users are strongly encouraged to avoid including this option in any production environment as data could be lost if improperly used.

### Variable, VariableData, and Dataset
Simply put, an `mdio::Variable` is the C++ representation of the [Dataset model](https://mdio-python.readthedocs.io/en/v1/data_models/version_1.html#mdio.schemas.v1.variable.Variable) Variable. It holds no array data, but will be used to both read and write. This process will be explained in more depth below.

An `mdio::VariableData` is what will be used to manipulate your data in-memory.

An `mdio::Dataset` represents a collection of Variables, and their relation with one another.

More information about Variables, their underlying constructs, and relation to XArray can be found [here](https://github.com/TGSAI/mdio-cpp/issues/8).

## Constructors
Constructors are based entirely off of the [MDIO v1](https://mdio-python.readthedocs.io/en/v1/data_models/version_1.html#reference) Dataset model. Simply specify a JSON schema and provide it to the `mdio::Dataset::from_json()` method along with the desired path (which can be a relative path, absolute path, or even a GCS or S3 path!), and your open options

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

A label must be a dimension as outlined by the **MDIO** [dataset V1 model](https://mdio-python.readthedocs.io/en/v1/data_models/version_1.html#mdio.schemas.v1.variable.Variable.dimensions). If the label does not apply to a Variable it will simply be overloked during the slice (no-op). This means you can safely slice an entire dataset, even if there are Variables with differing dimensions.

The recommended method to apply slices is at the dataset level. This will ensure consistent dimensions across your entire dataset.
1. Construct your slice descriptors.
2. Slice the dataset with the `isel` method.
3. Operate on the returned dataset.

Slicing is curcial when working on large datasets that would not fit into memory for reasons explained in the Read section below.

## Read
Reading **MDIO** is performed lazily, which means that we will only read a small amount of metadata from disk when the **MDIO** is opened. In order to read the array data from disk, we will need to actively solicit the read. It's important to note that once we solicit the read, all of the data will be read at once in parallel automatically. If the data is sliced before reading, only the [chunk grids](https://mdio-python.readthedocs.io/en/v1/data_models/version_1.html#mdio.schemas.v1.variable.VariableMetadata.chunk_grid) contained within the slice are read.

```C++
/** 
 * @brief Reads an arbitrary Variable into memory.
 * @return An Ok result if the process worked as expected.
 */
mdio::Result<void> read_and_return_result(mdio::Dataset& ds) {
  // Pick the first key in the keys list.
  std::string variableToRead = ds.variables.get_keys().front();
  // Get the mdio::Variable object
  MDIO_ASSIGN_OR_RETURN(auto variable, ds.variables.at(key));
  // Solicit the read. This happens asynchronously so we could do other things while the data is read.
  auto readFuture = variable.Read();
  return readFuture.status();  // We are forcing the future to block here.
}
```
This example isn't very useful on its own, so lets step it up by giving it a type and view the array.
```C++
/** 
 * @brief Reads an arbitrary Variable into memory.
 * @return An Ok result if the process worked as expected.
 */
mdio::Result<void> read_and_return_result(mdio::Dataset& ds) {
  // We elect to use the `get_iterable_accessor` because the order is sorted.
  std::string variableToRead = ds.variables.get_iterable_accessor().front();  // "Grid"
  // We use `.get<T>()` so we can use the `get_data_accessor()` in a simple and easy way.
  MDIO_ASSIGN_OR_RETURN(auto variable, ds.variables.get<mdio::dtypes::float32_t>(variableToRead));
  auto readFuture = variable.Read();
  if (!readFuture.status().ok()) {
    return readFuture.status();
  }
  auto variableData = readFuture.value();

  // This gives us our array
  auto arrayValues = variableData.get_data_accessor();

  for (mdio::Index x=0; x<10; x++) {
    for (mdio::Index y=0; y<10; y++) {
      std::cout << arrayValues({x, y}) << " ";
      // You may also use bracket notation here if you wish
      // std::cout << arrayValues[x][y] << " ";
    }
    std::cout << std::endl;
  }

  return absl::OkStatus();
}
```

## Write
Writing **MDIO** data happens in parallel automatically, just like reading. We also need to have either read the values, or generated them from an empty Variable.

```C++
mdio::Result<void> write_and_return_result(mdio::Dataset& ds) {
  std::string variableToWrite = ds.variables.get_iterable_accessor().front();  // "Grid"
  MDIO_ASSIGN_OR_RETURN(auto variable, ds.variables.get<mdio::dtypes::float32_t>(variableToWrite));
  auto readFuture = variable.Read();

  if (!readFuture.status().ok()) {
    return readFuture.status();
  }
  auto variableData = readFuture.value();

  auto arrayValues = variableData.get_data_accessor();

  float value = 0.0;
  for (mdio::Index x=0; x<10; x++) {
    for (mdio::Index y=0; y<10; y++) {
      // The access pattern is the same as before, but now we're setting instead of reading
      arrayValues({x, y}) = value++;
    }
  }

  // Writing is async, we could be performing more operations here if we wished.
  auto writeFuture = variable.Write(variableData);
  return writeFuture.status();
}
```
Since we are overwriting all of our data anyway, we can also use the `from_variable` function to get our `VariableData` object. This function is only recommended if you are the one originating all the data and must be used with caution. Not being chunk-aligned in writes using the `from_variable` method may result in undefined behavior and is not guarenteed to remain consistent between versions of mdio-cpp.

If you are ever in doubt, use the above method of reading to get the VariableData object.
```C++
mdio::Result<void> overwrite_and_return_result(mdio::Dataset& ds) {
  std::string variableToWrite = ds.variables.get_iterable_accessor().front();  // "Grid"
  MDIO_ASSIGN_OR_RETURN(auto variable, ds.variables.get<mdio::dtypes::float32_t>(variableToWrite));
  // This allows for shorter overall code and should avoid larger reads
  MDIO_ASSIGN_OR_RETURN(auto variableData, mdio::from_variable<mdio::dtypes::float32_t>(variable));

  auto arrayValues = variableData.get_data_accessor();

  float value = 0.0;
  for (mdio::Index x=0; x<10; x++) {
    for (mdio::Index y=0; y<10; y++) {
      arrayValues({x, y}) = value++;
    }
  }

  auto writeFuture = variable.Write(variableData);
  return writeFuture.status();
}
```

## Efficient Assignment (Advanced)
For small datasets, setting data elements one at a time may be reasonable, but as your dataset grows, so too does the time it takes to copy from one array to another. The typical way to handle this is to use the STL `std::memcpy` function. When dealing with full datasets, this works exactly as expected, copy from one address or container to another. If the dataset is sliced, as would be expected for large datasets, there is an additional challenge that is presented. When slicing outside of the logical origin, there is an offset in memory that must be taken into account. **MDIO** does provide a convienent method to getting that offset, but as with any low-level operation care must be taken.
```C++
mdio::Result<void> copy_overwrite_and_return(mdio::Dataset& ds) {
  std::string variableToWrite = ds.variables.get_iterable_accessor().front();  // "Grid"
  MDIO_ASSIGN_OR_RETURN(auto variable, ds.variables.get<mdio::dtypes::float32_t>(variableToWrite));
  MDIO_ASSIGN_OR_RETURN(auto variableData, mdio::from_variable<mdio::dtypes::float32_t>(variable));

  // We will get the raw pointer first, and then cast it to a properly typed pointer.
  auto voidPtr = variableData.get_data_accessor().data();  // Note the `.data()` call to get the raw pointer.
  auto typedPtr = static_cast<mdio::dtypes::float32_t*>(voidPtr); 

  // We'll generate some inert data to copy
  std::vector<mdio::dtypes::float32_t> fill_val(51200);
  for (auto i = 0; i < 51200; i++) {
    fill_val[i] = float(i*.0002);
  }

  const auto offsetValue = variableData.get_flattened_offset();
  std::size_t increment = 0;
  for (auto i=0; i<51200; i++) {
    std::memcpy(&typedPtr[offsetValue], fill_val.data(), sizeof(mdio::dtypes::float32_t)*51200);
    increment += 51200;
  }

  auto writeFuture = variable.Write(variableData);
  return writeFuture.status();
}
```
This example is good for demonstration purposes, but for a practical application it's too inflexible. Lets fix it by slicing our dataset and copying the data.
```C++
mdio::Result<void> copy_overwrite_and_return(mdio::Dataset& ds) {
  mdio::SliceDescriptor xSlice = {"X", 256, 512, 1};
  mdio::SliceDescriptor ySlice = {"Y", 512, 768, 1};

  MDIO_ASSIGN_OR_RETURN(auto slicedDs, ds.isel(xSlice, ySlice));

  std::string variableToWrite = slicedDs.variables.get_iterable_accessor().front();  // "Grid"
  MDIO_ASSIGN_OR_RETURN(auto variable, slicedDs.variables.get<mdio::dtypes::float32_t>(variableToWrite));
  MDIO_ASSIGN_OR_RETURN(auto variableData, mdio::from_variable<mdio::dtypes::float32_t>(variable));

  // We will get the raw pointer first, and then cast it to a properly typed pointer.
  auto voidPtr = variableData.get_data_accessor().data();  // Note the `.data()` call to get the raw pointer.
  auto typedPtr = static_cast<mdio::dtypes::float32_t*>(voidPtr); 

  auto numSamples = variable.num_samples();

  // We'll generate some inert data to copy
  std::vector<mdio::dtypes::float32_t> fill_val(numSamples);
  for (auto i = 0; i < numSamples; i++) {
    fill_val[i] = float(i*.0002);
  }

  const auto offsetValue = variableData.get_flattened_offset();
  std::size_t increment = 0;
  for (auto i=0; i<1; i++) {
    std::memcpy(&typedPtr[offsetValue], fill_val.data(), sizeof(mdio::dtypes::float32_t)*fill_val.size());
    increment += numSamples;
  }

  auto writeFuture = variable.Write(variableData);
  return writeFuture.status();
}
```

## Summary Statistics
You may have noticed that [summary statistics](https://mdio-python.readthedocs.io/en/v1/data_models/version_1.html#mdio.schemas.v1.stats.StatisticsMetadata) is part of the dataset model, but how can you include them in your Variable before you've even seen the data? This thought exercise assumes that the answer is "You can't!". To address this problem we allow a limited portion of the metadata to be changed at the Variable level.