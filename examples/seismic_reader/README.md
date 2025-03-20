# Seismic data reader

The purpose of this example is to demonstrate one method of integrating the **MDIO** C++ library into a seismic data reader into an existing project.

Due to the variety of target build systems and differing complexities of integrating the library with existing CMake projects, this example will also expose a way to build MDIO and its dependencies as a set of shared and linkable libraries and integrate it that way.

## Table of contents
- [Concepts demonstrated](#concepts-demonstrated)
- [Overview](#overview)
- [Running](#running)
- [Glossary](#glossary)

## Concepts demonstrated
- Index-based slicing
- Value-based slicing
- Dimension coordinates
- Coordinates
- Caching

Chunk-aligned opreations were briefly discussed but not examined in great detail for this example.

## Overview

This example assumes that you are able to build the main branch of **MDIO**.

This example will perform the following steps:

1. Clone the installer to this directory.
2. Run the installer, building and installing several archives and shared files.
3. Link the installed files in the `CMakeLists.txt`.
4. Demonstrate opening a Dataset with a configurable cache.
5. Demonstrate acquiring the corner points for the UTM grid on the Open Poseidon dataset from S3.
    - Calculates the latitude and longitude coordinates for the corner points.
    - Provides a web link to display the surface area on a map.
6. Demonstrate acquiring the inline and crossline extents.
7. Demonstrate an index-based slice for chunk-aligned, tracewise processing.
    - Calculates basic statistics for a small section of the dataset.
    - Tracks the highest (peak) and lowest (trough) amplitudes and their actual inline/crossline coordinate pairs.
8. Demonstrate a value-based slice for more targeted analysis.
    - Corolates the cdp-x and cdp-y coordinate pair for the peak and trough values.
    - Provides a web link to display each one's location on a map.

## Running

```bash
$ ./bootstrap.sh
$ cd build
$ cmake ..
$ make -j
```

The bootstrap shell script will build and install the mdio-cpp library as a set of shared and static objects.

It will then set up the build directory if it completes without error.

After the program has finished building it can be run with `./read`.

## Glossary
- Dimension coordinate: A 1-dimensional Variable that describes a dimension of the dataset. 
- Coordinate: A Variable that helps describe data outside of its natural (logical) domain. May be greater than 1-dimensional.
- Slicing: The act of subsetting a dataset along one or more dimension coordinates. A subset of a dataset is still considered a dataset.
- Index-based slicing (*isel*): Subsetting a dataset based on the logical indices of its *dimension coordinate*(s).
- Value-based slicing (*sel*): Subsetting a dataset based on the values contained by its *dimension coordinate*(s).
- Chunk-aligned: Slicing the data along its logical chunk boundries for efficient I/O performance.