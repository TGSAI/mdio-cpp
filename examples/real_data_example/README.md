# Seismic Data Extractor

A C++ tool for extracting and processing 3D seismic data slices from MDIO datasets. The tool will extract a slice of a larger MDIO dataset and output the result to a NumPy array called seismic_slice.npy.

This tool demonstrates working with the Poseidon dataset, a public seismic dataset available on AWS Earth, and is provided under a CC BY 4.0 license.

## Overview

This tool allows users to extract specific slices from 3D seismic data stored in MDIO format, with options to specify ranges for inline, crossline, and depth dimensions. The extracted data can be saved in NumPy format for further analysis.

## Prerequisites

- C++ compiler with C++17 support
- MDIO library (for reading seismic data)
- indicators library (for progress bars)
- CMake (for building)

## Installation

[Installation instructions would be valuable here]

## Usage

```bash
./real_data_example [flags]
```

### Flags

- `--dataset_path`: Path to the MDIO dataset (default: "s3://tgs-opendata-poseidon/full_stack_agc.mdio")
- `--inline_range`: Inline range in format {inline,start,end,step} (default: "{inline,700,701,1}")
- `--xline_range`: Crossline range in format {crossline,start,end,step} (default: "{crossline,500,700,1}")
- `--depth_range`: Optional depth range in format {depth,start,end,step}
- `--variable_name`: Name of the seismic variable to extract (default: "seismic")
- `--print_dataset`: Print the dataset URL and return without processing

### Example

To print the contents of the file ...
```
./real_data_example --print_dataset --dataset_path="s3://tgs-opendata-poseidon/full_stack_agc.mdio"
```

To slice the data and write to numpy ...
```
./real_data_example  --inline_range="{inline,700,701,1}"  --xline_range="{crossline,500,1000,1}" --depth_range="{time, 300, 900,1}"
```
