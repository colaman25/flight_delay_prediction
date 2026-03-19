import yaml
import os
from pyspark.sql import SparkSession, Row
from datetime import datetime
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, Imputer
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from xgboost.spark import SparkXGBRegressor
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
    .config("spark.hadoop.fs.s3a.endpoint.region", "eu-west-2")
    #.config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
    #.config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
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


def load_config(config_path='config.yaml'):
    """Loads configuration from a YAML file."""
    with open(config_path, 'r') as file:
        # Use safe_load for security when working with configuration files
        config = yaml.safe_load(file)
    return config


def train_model(config_path='config.yaml'):
    # Load configuration
    config = load_config(config_path)
    training_cfg = config['training_config']
    feature_cfg = config['feature_config']
    model_cfg = config['model_config']

    # Load Data
    df1 = spark.read.format(training_cfg["data_source"]).option("header", "true").option("inferSchema", "true").load(training_cfg['data_input_path'])

    # Extract a smaller dataset
    small_df1 = df1.sample(True, training_cfg['sample_rate'], seed=42)

    # Weight Adjustment for multiple entries per flight
    small_df1 = small_df1.withColumn(
        "flight_id",
        F.concat_ws(
            "_",
            F.col("ident"),
            F.col("origin_airport_icao"),
            F.col("scheduled_out").cast("string")
        )
    )

    small_df1 = small_df1.withColumn(
        "sample_weight",
        1.0 / F.count("*").over(Window.partitionBy("flight_id"))
    )

    # Build, Test & Evaluate Model
    # Define categorical and numeric columns
    categorical_cols = feature_cfg.get('categorical_cols', [])
    numerical_cols = feature_cfg.get('numerical_cols', [])
    label_col = feature_cfg['label_col']

    # Fill nulls in numeric columns with their mean value
    imputer = Imputer(
        inputCols=numerical_cols,
        outputCols=numerical_cols
    ).setStrategy("mean")

    # Train/test split
    train_df, test_df = small_df1.randomSplit([1 - training_cfg['test_split'], training_cfg['test_split']], seed=45)


    # Train Linear Regression model
    # Step 1: Index and one-hot encode categorical columns
    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep") for col in categorical_cols]
    encoders = [OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_vec") for col in categorical_cols]

    # Step 2: Assemble features into a single vector column
    assembler_inputs = [f"{col}_vec" for col in categorical_cols] + numerical_cols
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")

    # Step 3: Train Model
    lr = LinearRegression(featuresCol="features", labelCol=label_col, weightCol="sample_weight")

    # Step 4: Pipeline
    pipeline = Pipeline(stages=indexers + encoders + [imputer] + [assembler] + [lr])
    lr_model = pipeline.fit(train_df)

    # Step 5: Save Model
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    save_path = f"{training_cfg['model_output_path']}/linear_regression_{timestamp}"
    lr_model.write().overwrite().save(save_path)

    # Step 6: Evaluate and Save Metrics for LR
    evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction")
    predictions_lr = lr_model.transform(test_df)
    r2_lr = evaluator.evaluate(predictions_lr, {evaluator.metricName: "r2"})
    rmse_lr = evaluator.evaluate(predictions_lr, {evaluator.metricName: "rmse"})

    metrics_lr = {"Model": "LinearRegression", "R2": r2_lr, "RMSE": rmse_lr, "Timestamp": timestamp}
    metrics_df_lr = spark.createDataFrame([Row(**metrics_lr)])
    metrics_output_path_lr = f"{training_cfg['model_output_path']}/metrics_LinearRegression_{timestamp}.json"
    metrics_df_lr.coalesce(1).write.mode("overwrite").json(metrics_output_path_lr)


    # Decision Tree Regression
    # Step 1: Index and one-hot encode categorical columns
    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep") for col in categorical_cols]

    # Step 2: Assemble features into a single vector column
    assembler_inputs = [f"{col}_idx" for col in categorical_cols] + numerical_cols
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")

    # Step 3: Train Model
    dt = DecisionTreeRegressor(featuresCol="features", labelCol=label_col, weightCol="sample_weight", maxBins=model_cfg['decision_tree_max_bins'])

    # Step 3: Pipeline
    pipeline = Pipeline(stages=indexers + [imputer] + [assembler] + [dt])
    dt_model = pipeline.fit(train_df)

    # Step 5: Save Model
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    save_path = f"{training_cfg['model_output_path']}/decision_tree_regression_{timestamp}"
    dt_model.write().overwrite().save(save_path)

    # Step 6: Evaluate and Save Metrics for LR
    evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction")
    predictions_dt = dt_model.transform(test_df)
    r2_dt = evaluator.evaluate(predictions_dt, {evaluator.metricName: "r2"})
    rmse_dt = evaluator.evaluate(predictions_dt, {evaluator.metricName: "rmse"})

    metrics_dt = {"Model": "DecisionTreeRegression", "R2": r2_dt, "RMSE": rmse_dt, "Timestamp": timestamp}
    metrics_df_dt = spark.createDataFrame([Row(**metrics_dt)])
    metrics_output_path_dt = f"{training_cfg['model_output_path']}/metrics_DecisionTreeRegression_{timestamp}.json"
    metrics_df_dt.coalesce(1).write.mode("overwrite").json(metrics_output_path_dt)


    # XGBoost Regression
    # Step 1: Index categorical columns
    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep") for col in categorical_cols]

    # Step 2: Assemble features
    assembler_inputs = [f"{col}_idx" for col in categorical_cols] + numerical_cols
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")

    # Step 3: Train XGBoost model
    xgb = SparkXGBRegressor(
        features_col="features",
        label_col=label_col,
        weight_col="sample_weight",
        num_workers=2,
        max_depth=model_cfg.get("xgboost_max_depth", 6),
        eta=model_cfg.get("xgboost_eta", 0.3),
        objective="reg:squarederror"
    )

    # Step 4: Pipeline
    pipeline = Pipeline(stages=indexers + [imputer] + [assembler] + [xgb])
    xgb_model = pipeline.fit(train_df)

    # Step 5: Save Model
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    save_path = f"{training_cfg['model_output_path']}/xgboost_regression_{timestamp}"
    xgb_model.write().overwrite().save(save_path)

    # Step 6: Evaluate
    evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction")

    predictions_xgb = xgb_model.transform(test_df)
    r2_xgb = evaluator.evaluate(predictions_xgb, {evaluator.metricName: "r2"})
    rmse_xgb = evaluator.evaluate(predictions_xgb, {evaluator.metricName: "rmse"})

    metrics_xgb = {
        "Model": "XGBoostRegression",
        "R2": r2_xgb,
        "RMSE": rmse_xgb,
        "Timestamp": timestamp
    }

    metrics_df_xgb = spark.createDataFrame([Row(**metrics_xgb)])

    metrics_output_path_xgb = f"{training_cfg['model_output_path']}/metrics_XGBoostRegression_{timestamp}.json"
    metrics_df_xgb.coalesce(1).write.mode("overwrite").json(metrics_output_path_xgb)


train_model('config.yaml')