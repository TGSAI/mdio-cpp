import argparse
import sys
try:
    import xarray as xr
except ImportError:
    print("Failed to import xarray.")
    sys.exit(0xfd) # 64768


def test_xarray_dataset(file_path, consolidated_metadata):
  if consolidated_metadata == "True":
    consolidated_metadata = True
  else:
    consolidated_metadata = False
  try:
    ds = xr.open_zarr(
        file_path,
        consolidated=consolidated_metadata,
        mask_and_scale=False,
        chunks=None)
    return 0
  except Exception as e:
    print(f"Failed to open dataset: {e}")
    print(f"Exception type: {type(e).__name__}")
    return 0xff


if __name__ == "__main__":
  parser = argparse.ArgumentParser(
      description='Test opening an xarray dataset.')
  parser.add_argument(
      'file_path', type=str, help='The file path to the dataset.')
  parser.add_argument(
      'consolidated_metadata',
      type=str,
      help='Whether to use consolidated metadata.')
  args = parser.parse_args()

  sys.exit(test_xarray_dataset(args.file_path, args.consolidated_metadata))
