import yaml
import os
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import to_json, struct, col

WAREHOUSE_PATH = "s3a://flight-delay-predictions/iceberg"

spark = (
    SparkSession.builder
    .appName("PredictionJob")
    .master("local[*]")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
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


def load_config(config_path='config.yaml'):
    """Loads configuration from a YAML file."""
    with open(config_path, 'r') as file:
        # Use safe_load for security when working with configuration files
        config = yaml.safe_load(file)
    return config


def run_prediction_s(config_path='config.yaml'):
    # Load configuration
    config = load_config(config_path)
    predict_cfg = config['predict_config']

    # Load Model
    model = PipelineModel.load(predict_cfg['model_input_path'])

    # Load Input File
    df = spark.readStream.format("iceberg").load("local.data.combined_data")

    # Make Predictions
    predictions = model.transform(df)

    # Discard non original columns
    output_cols = [c for c in predictions.columns if not c.endswith("_idx") and not c.endswith("_vec") and c != "features"]

    # Save Prediction Result to Iceberg
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {predict_cfg['result_output_path']}(
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
        prediction DOUBLE
        )

        USING iceberg
        """)

    # Data Conversions for Kafka
    kafka_df = (
        predictions
            .select(*output_cols)
            .select(
            col("ident").cast("string").alias("key"),
            to_json(struct(*output_cols)).alias("value")
        )
    )

    iceberg_query = (
        predictions
            .select(*output_cols)
            .writeStream
            .format("iceberg")
            .outputMode("append")
            .option("checkpointLocation",
                    "checkpoints/data_prediction_data")
            .toTable(predict_cfg['result_output_path'])
    )

    kafka_query = (
        kafka_df
            .writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("topic", "prediction-results")
            .option("checkpointLocation", "checkpoints/data_prediction_kafka")
            .outputMode("append")
            .start()
    )

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run_prediction_s('config.yaml')