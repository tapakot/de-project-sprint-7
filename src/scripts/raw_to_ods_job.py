import sys

from pyspark.sql import SparkSession  # type: ignore
import pyspark.sql.functions as F  # type: ignore


def main():

    date = sys.argv[1]
    base_input_path = sys.argv[2]
    base_output_path = sys.argv[3]

    spark = (
        SparkSession.builder.appName(f"EventsGeoODSJob-{date}")
        .config(
            "spark.sql.sources.commitProtocolClass",
            "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol",
        )
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    events = spark.read.parquet(base_input_path).filter(F.col('date') == date)

    events.write.partitionBy('date', 'event_type').mode('overwrite').parquet(
        base_output_path
    )

    spark.stop()


if __name__ == "__main__":
    main()