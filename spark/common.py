"""Shared Spark session and paths for all Spark jobs."""
import json
import os
import sys
import time

from pyspark.sql import SparkSession

# Python workers (used for createDataFrame from Python data and UDFs) must run the SAME Python as this script.
# Without this, Spark used another Python on the machine (3.11) and refused to run with the 3.14 driver.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

RAW = os.getenv("DATA_DIR", "data")        # generator output (JSON arrays)
LAKE = os.getenv("LAKE_DIR", "lake")       # cleaned Parquet tables
OUT = "results/spark"


def spark_session(app):
    """Local Spark using all cores. Memory must be set here, before the JVM starts."""
    return (SparkSession.builder.appName(app).master("local[*]")
            .config("spark.driver.memory", os.getenv("SPARK_MEM", "6g"))
            .config("spark.sql.shuffle.partitions", "64")        # default 200 is too many for one machine
            .config("spark.sql.adaptive.enabled", "true")       # AQE: merges small partitions, handles skew
            .config("spark.sql.session.timeZone", "UTC")         # generator timestamps have no zone: treat as UTC
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate())


class Timer:
    """Collects wall-clock times of named steps and saves them."""
    def __init__(self):
        self.steps = {}

    def run(self, name, fn):
        t = time.perf_counter()
        out = fn()
        self.steps[name] = round(time.perf_counter() - t, 2)
        print(f"  {name:<40} {self.steps[name]:>8.2f} s")
        return out

    def save(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.steps, f, indent=2)
