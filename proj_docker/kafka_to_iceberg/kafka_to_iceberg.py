from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    DoubleType, LongType, TimestampType, IntegerType
)
import pyspark.sql.functions as F
import os

WAREHOUSE_PATH = "s3a://flight-delay-predictions/iceberg"

spark = (
    SparkSession.builder
    .appName("IcebergKafkaStreaming")
    .master("local[*]")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
    .config("spark.sql.defaultCatalog", "local")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS local.flights")
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.schedule")

spark.sql("""
CREATE TABLE IF NOT EXISTS local.flights.flight_positions (
    icao24 STRING,
    callsign STRING,
    origin_country STRING,
    time_position BIGINT,
    last_contact BIGINT,
    longitude DOUBLE,
    latitude DOUBLE,
    baro_altitude DOUBLE,
    on_ground BOOLEAN,
    velocity DOUBLE,
    true_track DOUBLE,
    vertical_rate DOUBLE,
    sensors STRING,
    geo_altitude DOUBLE,
    squawk STRING,
    spi BOOLEAN,
    position_source BIGINT
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS local.schedule.schedule_data (
    origin_code_icao STRING,
    dest_code_icao STRING,
    ident STRING,
    operator STRING,
    diverted BOOLEAN,
    cancelled BOOLEAN,
    registration STRING,
    scheduled_out TIMESTAMP,
    scheduled_off TIMESTAMP,
    estimated_out TIMESTAMP,
    estimated_off TIMESTAMP,
    actual_out TIMESTAMP,
    actual_off TIMESTAMP,
    scheduled_on TIMESTAMP,
    scheduled_in TIMESTAMP,
    estimated_on TIMESTAMP,
    estimated_in TIMESTAMP,
    actual_on TIMESTAMP,
    actual_in TIMESTAMP,
    departure_delay INT,
    arrival_delay INT
)
USING iceberg
""")

# Flight Data
flight_schema = StructType([
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("origin_country", StringType()),
    StructField("time_position", LongType()),
    StructField("last_contact", LongType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("baro_altitude", DoubleType()),
    StructField("on_ground", BooleanType()),
    StructField("velocity", DoubleType()),
    StructField("true_track", DoubleType()),
    StructField("vertical_rate", DoubleType()),
    StructField("sensors", StringType()),
    StructField("geo_altitude", DoubleType()),
    StructField("squawk", StringType()),
    StructField("spi", BooleanType()),
    StructField("position_source", LongType())
])

flight_kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "flight-data")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

flight_df = (
    flight_kafka_df
    .selectExpr("CAST(value AS STRING) AS json")
    .select(F.from_json("json", flight_schema).alias("data"))
    .select("data.*")
)

flight_query = (
    flight_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "s3a://flight-delay-predictions/checkpoints/flight_positions")
    .toTable("local.flights.flight_positions")
)

# For Displaying, delete later
flight_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()


# Schedule Data
schedule_kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "schedule-data")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

schedule_schema = StructType([
    StructField("origin", StructType([
        StructField("code_icao", StringType())
    ])),
    StructField("destination", StructType([
        StructField("code_icao", StringType())
    ])),
    StructField("ident", StringType()),
    StructField("operator", StringType()),
    StructField("diverted", BooleanType()),
    StructField("cancelled", BooleanType()),
    StructField("registration", StringType()),
    StructField("scheduled_out", StringType()),
    StructField("scheduled_off", StringType()),
    StructField("estimated_out", StringType()),
    StructField("estimated_off", StringType()),
    StructField("actual_out", StringType()),
    StructField("actual_off", StringType()),
    StructField("scheduled_on", StringType()),
    StructField("scheduled_in", StringType()),
    StructField("estimated_on", StringType()),
    StructField("estimated_in", StringType()),
    StructField("actual_on", StringType()),
    StructField("actual_in", StringType()),
    StructField("departure_delay", IntegerType()),
    StructField("arrival_delay", IntegerType())
])

schedule_df = (
    schedule_kafka_df
    .selectExpr("CAST(value AS STRING) AS json")
    .select(F.from_json("json", schedule_schema).alias("data"))
    .select(
        F.col("data.origin.code_icao").alias("origin_code_icao"),
        F.col("data.destination.code_icao").alias("dest_code_icao"),
        "data.ident",
        "data.operator",
        "data.diverted",
        "data.cancelled",
        "data.registration",
        F.to_timestamp("data.scheduled_out").alias("scheduled_out"),
        F.to_timestamp("data.scheduled_off").alias("scheduled_off"),
        F.to_timestamp("data.estimated_out").alias("estimated_out"),
        F.to_timestamp("data.estimated_off").alias("estimated_off"),
        F.to_timestamp("data.actual_out").alias("actual_out"),
        F.to_timestamp("data.actual_off").alias("actual_off"),
        F.to_timestamp("data.scheduled_on").alias("scheduled_on"),
        F.to_timestamp("data.scheduled_in").alias("scheduled_in"),
        F.to_timestamp("data.estimated_on").alias("estimated_on"),
        F.to_timestamp("data.estimated_in").alias("estimated_in"),
        F.to_timestamp("data.actual_on").alias("actual_on"),
        F.to_timestamp("data.actual_in").alias("actual_in"),
        "data.departure_delay",
        "data.arrival_delay"
    )
)

schedule_query = (
    schedule_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "s3a://flight-delay-predictions/checkpoints/schedule_data")
    .toTable("local.schedule.schedule_data")
)

# For Displaying, delete later
schedule_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

spark.streams.awaitAnyTermination()