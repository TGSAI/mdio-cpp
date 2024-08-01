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
import zarr


def test_zarr_dataset(file_path):
  try:
    z = zarr.open(file_path)
    return 0
  except Exception as e:
    print(f"Failed to open Variable: {e}")
    return 0xff


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Test opening a zarr dataset.')
  parser.add_argument(
      'file_path', type=str, help='The file path to the dataset.')
  args = parser.parse_args()

  sys.exit(test_zarr_dataset(args.file_path))
