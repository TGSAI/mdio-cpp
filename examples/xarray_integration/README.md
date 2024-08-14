# XArray Integration Project

This project integrates XArray with MDIO datasets, providing both a command-line tool and a Python script to ensure that image coordinates (`inline`, `crossline`, `depth`) are properly populated in your datasets.

This example creates a mdio dataset using the C++ API and populates values in the coords. We then
check that these are read correctly using Python's Xarray. To complete the roundtrip, the first 
row of the "image" variable is populated with 1's and we use the C++ API to validate that.

Follow the instructions to complete the example.

## Setup Instructions

### Prerequisites

Before starting, make sure you have the following installed:

- **CMake**: To build the C++ components.
- **Poetry**: For managing Python dependencies.

### Step 1: Create the Build Directory

Begin by creating a `build` directory where the build files will be generated.

```sh
mkdir build
cd build
cmake ..
make -j${nproc} xarray_integration mdio_from_xarray
cd ..
```

### Step 2: Install Python Dependencies

```sh
poetry install
```

### Step 3: Generate some example inputs using the C++ app

```sh
build/xarray_integration --PATH=test.mdio
```

### Step 4: View the MDIO data in XArray

```sh
poetry run xarray-integration --path=test.mdio
```

### Expected Outputs

```python
inline = [1000 2000 3000]
crossline = [6120 5120 4120 3120 2120 1120]
depth = [0 1000 2000 3000]
```

### Step 5: Complete the round-trip validate the first row of image is '1'

build/mdio_from_xarray --PATH=test.mdio


