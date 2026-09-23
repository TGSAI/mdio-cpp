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

import argparse
import os
import sys
import time

try:
    import zarr
    from segy.schema import HeaderField
    from segy.standards import get_segy_standard

    import mdio
    from mdio import segy_to_mdio, open_mdio
    from mdio.builder.schemas.v1.units import TimeUnitModel
    from mdio.builder.template_registry import get_template
except ImportError as e:
    print(f"Failed to import required packages: {e}")
    sys.exit(0xfd)


def _apply_requested_zarr_format():
    """Honor the Zarr format requested via ZARR_DEFAULT_ZARR_FORMAT.

    Zarr reads this environment variable at import time, but `import mdio`
    above already imports zarr, so we re-apply it explicitly here to make sure
    the requested format wins regardless of import ordering.
    """
    zarr_format = os.environ.get("ZARR_DEFAULT_ZARR_FORMAT")
    if zarr_format is not None:
        zarr.config.set({"default_zarr_format": int(zarr_format)})


def test_multidimio_ingestion(output_path):
    os.environ["MDIO__IMPORT__CLOUD_NATIVE"] = "true"
    os.environ["MDIO__IMPORT__SAVE_SEGY_FILE_HEADER"] = "true"

    _apply_requested_zarr_format()
    print(f"Using Zarr format: {zarr.config.get('default_zarr_format')}")

    # Soda Lake 2010 public shot record (GDR / AWS Open Data). Replaces the
    # retired Teapot Dome filt_mig.sgy used by older mdio-python docs.
    input_url = (
        "https://gdr-data-lake.s3.us-west-2.amazonaws.com/"
        "soda_lake/raw_seismic/2010/v1.0.0/F7733R1.SGY"
    )

    print(f"Ingesting remote SEG-Y: {input_url} to {output_path}")

    shot_trace_headers = [
        HeaderField(name="shot_point", byte=9, format="int32"),
        HeaderField(name="channel", byte=13, format="int32"),
    ]

    rev1_segy_spec = get_segy_standard(1.0)
    soda_lake_segy_spec = rev1_segy_spec.customize(
        trace_header_fields=shot_trace_headers
    )

    mdio_template = get_template("StreamerShotGathers2D")
    unit_ms = TimeUnitModel(time="ms")
    mdio_template.add_units({"time": unit_ms})

    try:
        # Ingest. The remote SEG-Y fetch can hit transient HTTP errors, so retry
        # a few times before giving up.
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                segy_to_mdio(
                    input_path=input_url,
                    output_path=output_path,
                    segy_spec=soda_lake_segy_spec,
                    mdio_template=mdio_template,
                    overwrite=True,
                )
                break
            except Exception as ingest_error:  # noqa: BLE001
                if attempt == max_attempts:
                    raise
                print(
                    f"Ingestion attempt {attempt}/{max_attempts} failed "
                    f"({type(ingest_error).__name__}: {ingest_error}); retrying..."
                )
                time.sleep(2 * attempt)
        print("Ingestion successful.")

        # Open and read
        print(f"Opening ingested MDIO dataset: {output_path}")
        dataset = open_mdio(output_path)
        print("Dataset opened successfully.")
        print("Sizes:", dataset.sizes)

        # Verify we can read data variables
        amp_sample = dataset["amplitude"].isel(
            shot_point=0, channel=0, time=slice(0, 5)
        ).values
        print("Sample amplitude data:", amp_sample)

        print("Multidimio ingestion and read validation passed.")
        return 0
    except Exception as e:
        print(f"Failed during multidimio compatibility test: {e}")
        print(f"Exception type: {type(e).__name__}")
        return 0xff


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Test multidimio ingestion and read."
    )
    parser.add_argument(
        "output_path", type=str, help="The output path for the ingested MDIO dataset."
    )
    args = parser.parse_args()

    sys.exit(test_multidimio_ingestion(args.output_path))
