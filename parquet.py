import datetime
import json
import os
import shutil
import tempfile
import uuid

import pandas as pd

# the default msgpack implementation does not support dates
import umsgpack


def date_handler(ext):
    fields = [int(field) for field in ext.data.split(b"-")]
    return datetime.date(*fields)


def convert_to_parquet_from_msgpack(data):
    handlers = {10: date_handler}
    obj = umsgpack.unpackb(data, ext_handlers=handlers)
    with tempfile.TemporaryDirectory() as outdir:
        filename = os.path.join(outdir, "pq")

        try:
            # DEV: PyArrow's type inference/conversion can be touchy.
            #      The safest default is to present all columns as the
            #      "object" type by default.
            pd.DataFrame.from_dict(
                obj,
                dtype="object",
            ).to_parquet(
                filename,
                allow_truncated_timestamps=True,
                compression="snappy",
            )
        except:
            # If the conversion to Parquet fails, attempt to dump the erroneous
            # object to a file for inspection.
            with open(f"error-{uuid.uuid4()}.json", "w") as fd:
                json.dump(obj, fd, default=str)
            raise

        with open(filename, "rb") as infile:
            data = infile.read()
    return data


def convert_to_parquet_from_file(path):
    path = path.decode()

    with tempfile.TemporaryDirectory() as outdir:
        filename = os.path.join(outdir, "pq")

        try:
            # DEV: PyArrow's type inference/conversion can be touchy.
            #      The safest default is to present all columns as the
            #      "object" type by default.
            pd.read_json(
                path,
                dtype="object",
                lines=True,
            ).to_parquet(
                filename,
                allow_truncated_timestamps=True,
                compression="snappy",
            )
        except:
            # If the conversion to Parquet fails, attempt to dump the erroneous
            # object to a file for inspection.
            shutil.copyfile(path, f"error-{uuid.uuid4()}.json")
            raise

        with open(filename, "rb") as infile:
            data = infile.read()
    return data
