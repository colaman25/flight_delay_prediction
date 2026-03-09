import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql import Column
from pyspark.sql.window import Window
import holidays

# =========================================================
# Spark + Iceberg setup (same as your batch job)
# =========================================================

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
    #.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    #.config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.path.style.access", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    #.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =========================================================
# Constants & helpers
# =========================================================

EARTH_RADIUS_KM = 6371.0

def calculate_haversine_distance(df, lat1, lon1, lat2, lon2):
    lat1 = F.radians(F.col(lat1))
    lon1 = F.radians(F.col(lon1))
    lat2 = F.radians(F.col(lat2))
    lon2 = F.radians(F.col(lon2))

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        F.sin(dlat / 2) ** 2
        + F.cos(lat1) * F.cos(lat2) * F.sin(dlon / 2) ** 2
    )

    return 2 * F.asin(F.sqrt(a)) * F.lit(EARTH_RADIUS_KM)

def heading_deg(lon1: str, lat1: str, lon2: str, lat2: str) -> Column:
    # Compute heading (0–360 degrees) from (lon1, lat1) to (lon2, lat2)
    return (
        (
            F.degrees(
                F.atan2(
                    F.sin(F.radians(F.col(lon2) - F.col(lon1))) *
                    F.cos(F.radians(F.col(lat2))),

                    F.cos(F.radians(F.col(lat1))) *
                    F.sin(F.radians(F.col(lat2))) -
                    F.sin(F.radians(F.col(lat1))) *
                    F.cos(F.radians(F.col(lat2))) *
                    F.cos(F.radians(F.col(lon2) - F.col(lon1)))
                )
            ) + 360
        ) % 360
    )

# Holiday Table
uk = holidays.country_holidays("GB", years=[2026, 2027, 2028])
holiday_df = spark.createDataFrame(
    [(str(d),) for d in uk.keys()],
    ["holiday_date"]
).withColumn("holiday_date", F.to_date("holiday_date"))


# =========================================================
# Static reference data (loaded once)
# =========================================================

# Airport lat/long
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.airport.longlat(
    ident STRING,
    latitude_deg DOUBLE,
    longitude_deg DOUBLE
    )
    USING iceberg
""")

df_airport = spark.read.csv('airport_longlat.csv', header=True)
df_airport = df_airport.select(
    F.col('ident'),
    F.col('latitude_deg'),
    F.col('longitude_deg')
)

df_airport = df_airport.withColumn('latitude_deg', F.col('latitude_deg').cast(DoubleType()))\
                        .withColumn('longitude_deg', F.col('longitude_deg').cast(DoubleType()))

df_airport.write.format("iceberg") \
    .mode("overwrite") \
    .save("local.airport.longlat")

df_airport = (
    spark.read
    .format("iceberg")
    .load("local.airport.longlat")
)

# Aircraft registration ↔ ICAO24 mapping
df_conv = spark.read.csv(
    "aircraft-database-complete-2025-08.csv", header=True
)

df_conv = (
    df_conv
    .withColumn("icao24_cln",
                F.regexp_replace(F.col("'icao24'"), "\\'", ""))
)

for c in df_conv.columns:
    df_conv = df_conv.withColumnRenamed(c, c.strip("'"))

df_conv = df_conv.select("icao24_cln", "registration")


# Operator Average Delay Update
spark.sql("""
CREATE TABLE IF NOT EXISTS local.ref.operator_avg_delay (
    operator STRING,
    operator_avg_departure_delay DOUBLE
)
USING iceberg
""")

operator_avg = (
    spark.read.format("iceberg")
    .load("local.data.combined_data")
    .groupBy("operator")
    .agg(F.avg("departure_delay").alias("operator_avg_departure_delay"))
)

operator_avg.write.format("iceberg").mode("overwrite").save("local.ref.operator_avg_delay")

df_operator_avg = (
    spark.read
    .format("iceberg")
    .load("local.ref.operator_avg_delay")
)

global_avg_delay = (
    df_operator_avg
    .select(F.avg("operator_avg_departure_delay"))
    .first()[0]
)


# =========================================================
# Streaming sources (Iceberg)
# =========================================================

df_schedule_stream = (
    spark.readStream
    .format("iceberg")
    .load("local.schedule.schedule_data")
)

df_flight_stream = (
    spark.readStream
    .format("iceberg")
    .load("local.flights.flight_positions")
)

df_flight_stream = df_flight_stream.withColumn(
        "time_position_iso",
        F.from_unixtime(F.col("time_position")).cast("timestamp")
    )

# =========================================================
# Enrich schedule stream with airport data (stream–static)
# =========================================================

df_schedule_enriched = (
    df_schedule_stream
    .join(
        F.broadcast(df_airport),
        df_schedule_stream.origin_code_icao == df_airport.ident,
        "left"
    )
    .drop(df_airport.ident)
    .withColumnRenamed("origin_code_icao", "origin_airport_icao")
    .withColumnRenamed("dest_code_icao", "dest_airport_icao")
    .withColumnRenamed("latitude_deg", "airport_lat")
    .withColumnRenamed("longitude_deg", "airport_long")
)

# =========================================================
# Watermarks for stream–stream join
# =========================================================

df_schedule_wm = (
    df_schedule_enriched
    .withColumn("schedule_event_ts", F.col("scheduled_out"))
    .withWatermark("schedule_event_ts", "8 hours")
)

df_flight_wm = (
    df_flight_stream
    .withColumn(
        "flight_event_ts",
        F.to_timestamp(F.from_unixtime("time_position"))
    )
    .withWatermark("flight_event_ts", "8 hours")
)

# =========================================================
# Join logic (matches batch semantics)
# =========================================================

df_joined = (
    df_schedule_wm
    .join(
        df_conv,
        "registration",
        "left"
    )
    .join(
        df_flight_wm,
        (df_flight_wm.icao24 == df_conv.icao24_cln) &
        (df_flight_wm.time_position_iso >= df_schedule_enriched.scheduled_out - F.expr("INTERVAL 24 HOURS")) &
        (df_flight_wm.time_position_iso <= df_schedule_enriched.scheduled_out + F.expr("INTERVAL 24 HOURS")),
        "inner"
    )
)

# =========================================================
# Feature engineering
# =========================================================

df_features = (
    df_joined
    .withColumn(
        "aircraft_airport_distance_km",
        F.round(
            calculate_haversine_distance(
                df_joined,
                "airport_lat", "airport_long",
                "latitude", "longitude"
            ),
            2
        )
    )
    .withColumn(
        "seconds_to_scheduled_out",
        F.col("scheduled_out").cast("long") - F.col("time_position")
    )
    .withColumn(
        "on_ground_int",
        F.col("on_ground").cast("integer")
    )
    .withColumn(
        "diverted_int",
        F.col("diverted").cast("integer")
    )
    .withColumn(
        "cancelled_int",
        F.col("cancelled").cast("integer")
    )
    .withColumn(
        "aircraft_to_airport_heading",
        heading_deg('longitude', 'latitude', 'airport_long', 'airport_lat')
    )
    .withColumn(
        "scheduled_out_hour_of_day",
        F.hour("scheduled_out")
    )
    .withColumn(
        "scheduled_out_day_of_week",
        F.dayofweek("scheduled_out")
    )
    .withColumn(
        "scheduled_out_month_of_year",
        F.month("scheduled_out")
    )
)

df_features = (
    df_features
    .withColumn("scheduled_date", F.to_date("scheduled_out"))
    .join(
        F.broadcast(holiday_df),
        F.col("scheduled_date") == holiday_df.holiday_date,
        "left"
    )
    .withColumn(
        "is_holiday",
        F.when(F.col("holiday_date").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    )
    .drop("holiday_date", "scheduled_date")
)

df_features = (
    df_features
    .join(
        F.broadcast(df_operator_avg),
        "operator",
        "left"
    )
    .withColumn(
        "operator_avg_departure_delay",
        F.coalesce(
            F.col("operator_avg_departure_delay"),
            F.lit(global_avg_delay)
        )
    )
)


# =========================================================
# Filter training set (same logic as batch)
# =========================================================

df_trainset = (
    df_features
    .filter(F.col("time_position_iso") < F.col("actual_off"))
)

# =========================================================
# Sink: Iceberg (recommended)
# =========================================================

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.data.combined_data (
    origin_airport_icao STRING,
    dest_airport_icao STRING,
    ident STRING,
    operator STRING,
    diverted BOOLEAN,
    cancelled BOOLEAN,
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
    arrival_delay INT,
    airport_lat DOUBLE,
    airport_long DOUBLE,
    icao24_cln STRING,
    registration STRING,
    baro_altitude DOUBLE,
    callsign STRING,
    geo_altitude DOUBLE,
    icao24 STRING,
    last_contact BIGINT,
    latitude DOUBLE,
    longitude DOUBLE,
    on_ground BOOLEAN,
    origin_country STRING,
    position_source BIGINT,
    sensors STRING,
    spi BOOLEAN,
    squawk STRING,
    time_position BIGINT,
    true_track DOUBLE,
    velocity DOUBLE,
    vertical_rate DOUBLE,
    time_position_iso TIMESTAMP,
    aircraft_airport_distance_km DOUBLE,
    seconds_to_scheduled_out BIGINT,
    on_ground_int INT,
    diverted_int INT,
    cancelled_int INT,
    aircraft_to_airport_heading DOUBLE,
    scheduled_out_hour_of_day INT,
    scheduled_out_day_of_week INT,
    scheduled_out_month_of_year INT,
    operator_avg_departure_delay DOUBLE,
    is_holiday INT
    )
    
    USING iceberg
    """)

OUTPUT_COLUMNS = [
    "origin_airport_icao",
    "dest_airport_icao",
    "ident",
    "operator",
    "diverted",
    "cancelled",
    "scheduled_out",
    "scheduled_off",
    "estimated_out",
    "estimated_off",
    "actual_out",
    "actual_off",
    "scheduled_on",
    "scheduled_in",
    "estimated_on",
    "estimated_in",
    "actual_on",
    "actual_in",
    "departure_delay",
    "arrival_delay",
    "airport_lat",
    "airport_long",
    "icao24_cln",
    "registration",
    "baro_altitude",
    "callsign",
    "geo_altitude",
    "icao24",
    "last_contact",
    "latitude",
    "longitude",
    "on_ground",
    "origin_country",
    "position_source",
    "sensors",
    "spi",
    "squawk",
    "time_position",
    "true_track",
    "velocity",
    "vertical_rate",
    "time_position_iso",
    "aircraft_airport_distance_km",
    "seconds_to_scheduled_out",
    "on_ground_int",
    "diverted_int",
    "cancelled_int",
    "aircraft_to_airport_heading",
    "scheduled_out_hour_of_day",
    "scheduled_out_day_of_week",
    "scheduled_out_month_of_year",
    "operator_avg_departure_delay",
    "is_holiday"
]

df_trainset = df_trainset.select(*OUTPUT_COLUMNS)

query = (
    df_trainset.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "s3a://flight-delay-predictions/checkpoints/data_combined_data")
    .toTable("local.data.combined_data")
)

query.awaitTermination()