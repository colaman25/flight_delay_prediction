import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    DoubleType, LongType, IntegerType
)
import pyspark.sql.functions as F
from awsglue.context import GlueContext
from awsglue.job import Job

# =========================================================
# Glue job bootstrap
# =========================================================
# Iceberg/Glue-Catalog configuration (spark.sql.extensions,
# spark.sql.catalog.local.*, --datalake-formats) is set via job
# parameters in the Glue job definition (see infra/glue_jobs.tf),
# not programmatically here — those settings must exist before
# this SparkContext is created, which Glue does internally before
# any of this script's code runs.

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'warehouse_path',
    'kafka_bootstrap_servers',
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.sparkContext.setLogLevel("WARN")

WAREHOUSE_PATH = args['warehouse_path']
KAFKA_BOOTSTRAP_SERVERS = args['kafka_bootstrap_servers']

# MSK IAM auth — requires the aws-msk-iam-auth jar on the job's
# classpath (added via --extra-jars in the Glue job definition).
KAFKA_AUTH_OPTIONS = {
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "AWS_MSK_IAM",
    "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
}

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
CREATE TABLE IF NOT EXISTS local.schedule.departure_schedule_data (
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

spark.sql("""
CREATE TABLE IF NOT EXISTS local.schedule.arrival_schedule_data (
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
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .options(**KAFKA_AUTH_OPTIONS)
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
    .option("checkpointLocation", f"{WAREHOUSE_PATH}/checkpoints/flight_positions")
    .toTable("local.flights.flight_positions")
)

# Schedule Data (shared schema for departures and arrivals)
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

def parse_schedule_stream(kafka_df):
    return (
        kafka_df
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

# Departure Schedule Data
departure_kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .options(**KAFKA_AUTH_OPTIONS)
    .option("subscribe", "departure-schedule-data")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

departure_df = parse_schedule_stream(departure_kafka_df)

departure_query = (
    departure_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", f"{WAREHOUSE_PATH}/checkpoints/departure_schedule_data")
    .toTable("local.schedule.departure_schedule_data")
)

# Arrival Schedule Data
arrival_kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .options(**KAFKA_AUTH_OPTIONS)
    .option("subscribe", "arrival-schedule-data")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

arrival_df = parse_schedule_stream(arrival_kafka_df)

arrival_query = (
    arrival_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", f"{WAREHOUSE_PATH}/checkpoints/arrival_schedule_data")
    .toTable("local.schedule.arrival_schedule_data")
)

spark.streams.awaitAnyTermination()

# Only reached if the streaming queries above terminate (error or
# explicit stop) — this is a continuous streaming job, not a batch one.
job.commit()
