from __future__ import annotations

import sys
import time
from datetime import datetime
from os import getenv

import dotenv
import pyspark.sql.functions as F
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException, CapturedException

sys.path.append("/app")
from src.logger import LogManager

log = LogManager().get_logger(name=__name__)
dotenv.load_dotenv()


def main() -> ...:
    spark = (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("APP")
        .config(
            map={
                "spark.hadoop.fs.s3a.access.key": getenv("S3_ACCESS_KEY_ID"),
                "spark.hadoop.fs.s3a.secret.key": getenv("S3_SECRET_ACCESS_KEY"),
                "spark.hadoop.fs.s3a.endpoint": getenv("S3_ENDPOINT_URL"),
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            }
        )
        .getOrCreate()
    )

    df = spark.read.parquet(
        "s3a://data-ice-lake-05/master/data/source/messenger-yp/events/event_type=message"
    ).where(F.col("date") == "2022-01-15")

    df.explain(mode="cost")

    df.show(100)


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)
