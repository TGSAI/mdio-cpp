# Copyright 2024 TGS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import click
import xarray as xr


def print_image_coordinates(dataset):
    for coord in ["inline", "crossline", "depth"]:
        if coord in dataset.coords:
            print(f"{coord} decimated coords: {dataset.coords[coord].values[::100]}\n")
        else:
            print(f"{coord} coordinate not found in the dataset.")


@click.command()
@click.option("--path", default="test.mdio", type=click.Path(exists=True))
def main(path):
    dataset = xr.open_zarr(path, consolidated=True, mask_and_scale=False, chunks=None)

    print(f"{dataset} \n")

    print("Test that the coords are populated for the image variable:\n")
    print_image_coordinates(dataset["image"])

    # test initialization ...
    dataset["image"].data[:] = 1.0

    # commit write to disk
    dataset.to_zarr(path, mode="w")


if __name__ == "__main__":
    main()
