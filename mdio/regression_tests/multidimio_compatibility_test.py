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

try:
    from segy.schema import HeaderField
    from segy.standards import get_segy_standard

    import mdio
    from mdio import segy_to_mdio, open_mdio
    from mdio.builder.schemas.v1.units import TimeUnitModel
    from mdio.builder.template_registry import get_template
except ImportError as e:
    print(f"Failed to import required packages: {e}")
    sys.exit(0xfd)


def test_multidimio_ingestion(output_path):
    os.environ["MDIO__IMPORT__CLOUD_NATIVE"] = "true"
    os.environ["MDIO__IMPORT__SAVE_SEGY_FILE_HEADER"] = "true"

    input_url = "http://s3.amazonaws.com/teapot/filt_mig.sgy"

    print(f"Ingesting remote SEG-Y: {input_url} to {output_path}")

    teapot_trace_headers = [
        HeaderField(name="inline", byte=181, format="int32"),
        HeaderField(name="crossline", byte=185, format="int32"),
        HeaderField(name="cdp_x", byte=189, format="int32"),
        HeaderField(name="cdp_y", byte=193, format="int32"),
    ]

    rev0_segy_spec = get_segy_standard(0)
    teapot_segy_spec = rev0_segy_spec.customize(trace_header_fields=teapot_trace_headers)

    mdio_template = get_template("PostStack3DTime")
    unit_ms = TimeUnitModel(time="ms")
    mdio_template.add_units({"time": unit_ms})

    try:
        # Ingest
        segy_to_mdio(
            input_path=input_url,
            output_path=output_path,
            segy_spec=teapot_segy_spec,
            mdio_template=mdio_template,
            overwrite=True,
        )
        print("Ingestion successful.")

        # Open and read
        print(f"Opening ingested MDIO dataset: {output_path}")
        dataset = open_mdio(output_path)
        print("Dataset opened successfully.")
        print("Sizes:", dataset.sizes)

        # Verify we can read data variables
        amp_sample = dataset["amplitude"].isel(inline=0, crossline=0, time=slice(0, 5)).values
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
