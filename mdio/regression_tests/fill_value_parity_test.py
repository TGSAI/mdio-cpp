# Copyright 2026 TGS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate a reference MDIO dataset covering every supported scalar dtype.

This script is the source of truth for the C++ fill-value parity acceptance
test. It uses the mdio-python builder to create one variable per scalar dtype
that mdio-cpp supports and writes the dataset to Zarr (V2 or V3, selected via
the ZARR_DEFAULT_ZARR_FORMAT environment variable).

The C++ test then reads the fill values mdio-python wrote to disk and asserts
that mdio-cpp produces the same values for the same dtypes. Keeping the expected
values in mdio-python (rather than hardcoding them in C++) means the test tracks
mdio-python automatically as the source of truth.

Usage:
    python fill_value_parity_test.py <output_path>
"""

import argparse
import os
import sys

# Scalar dtypes supported by both mdio-python and mdio-cpp. The variable names
# encode the dtype ("v_<dtype>") so the C++ side can rediscover them on disk.
SCALAR_TYPE_NAMES = [
    "bool",
    "int8",
    "int16",
    "int32",
    "int64",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "float16",
    "float32",
    "float64",
    "complex64",
    "complex128",
]


def build_reference(output_path: str) -> int:
    try:
        import zarr
        from mdio.builder.dataset_builder import MDIODatasetBuilder
        from mdio.builder.schemas.chunk_grid import RegularChunkGrid
        from mdio.builder.schemas.chunk_grid import RegularChunkShape
        from mdio.builder.schemas.dtype import ScalarType
        from mdio.builder.schemas.v1.variable import VariableMetadata
        from mdio.builder.xarray_builder import to_xarray_dataset
    except ImportError as e:
        print(f"Failed to import mdio/zarr: {e}")
        return 0xFD  # Treated as "skip" by the harness when deps are missing.

    zarr_format = int(os.environ.get("ZARR_DEFAULT_ZARR_FORMAT", "2"))
    zarr.config.set({"default_zarr_format": zarr_format})

    builder = MDIODatasetBuilder("fill_value_parity")
    builder.add_dimension("dim0", 4)
    builder.add_coordinate(
        "dim0", dimensions=("dim0",), data_type=ScalarType.INT32
    )

    for name in SCALAR_TYPE_NAMES:
        scalar_type = ScalarType(name)
        chunk_grid = RegularChunkGrid(
            configuration=RegularChunkShape(chunk_shape=(4,))
        )
        builder.add_variable(
            name=f"v_{name}",
            dimensions=("dim0",),
            data_type=scalar_type,
            metadata=VariableMetadata(chunk_grid=chunk_grid),
        )

    dataset = builder.build()
    xr_dataset = to_xarray_dataset(dataset)
    # mdio-cpp's Dataset::Open relies on consolidated metadata (.zmetadata) for
    # Zarr V2; V3 uses the native zarr.json hierarchy and is not consolidated.
    xr_dataset.to_zarr(
        output_path,
        mode="w",
        zarr_format=zarr_format,
        consolidated=(zarr_format == 2),
    )
    print(
        f"Wrote fill-value reference dataset (zarr v{zarr_format}) to "
        f"{output_path}"
    )
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate an all-dtype MDIO reference dataset."
    )
    parser.add_argument(
        "output_path", type=str, help="Destination path for the dataset."
    )
    args = parser.parse_args()
    sys.exit(build_reference(args.output_path))
