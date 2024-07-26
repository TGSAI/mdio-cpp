# Copyright 2024 TGS
 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
 
# http://www.apache.org/licenses/LICENSE-2.0
 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import sys
import xarray as xr

def test_xarray_dataset(file_path, consolidated_metadata):
    if consolidated_metadata == "True":
        consolidated_metadata = True
    else:
        consolidated_metadata = False
    try:
        ds = xr.open_zarr(file_path, consolidated=consolidated_metadata, mask_and_scale=False, chunks=None)
        return 0
    except Exception as e:
        print(f"Failed to open dataset: {e}")
        print(f"Exception type: {type(e).__name__}")
        return 0xff

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test opening an xarray dataset.')
    parser.add_argument('file_path', type=str, help='The file path to the dataset.')
    parser.add_argument('consolidated_metadata', type=str, help='Whether to use consolidated metadata.')
    args = parser.parse_args()

    sys.exit(test_xarray_dataset(args.file_path, args.consolidated_metadata))