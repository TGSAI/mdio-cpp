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
