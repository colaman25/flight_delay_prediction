# Network connection Glue jobs attach to for VPC access (reaching MSK
# inside the VPC). Bound to one subnet/AZ, per how Glue Connections work.
resource "aws_glue_connection" "vpc" {
  name            = "${var.project_name}-${var.environment}-vpc-connection"
  connection_type = "NETWORK"

  physical_connection_requirements {
    subnet_id              = aws_subnet.main[0].id
    security_group_id_list = [aws_security_group.compute.id]
    availability_zone      = var.azs[0]
  }
}

locals {
  glue_script_prefix     = "s3://${aws_s3_bucket.warehouse.bucket}/glue-scripts"
  glue_reference_prefix  = "s3://${aws_s3_bucket.warehouse.bucket}/reference-data"
  iceberg_warehouse_path = "s3://${aws_s3_bucket.warehouse.bucket}/iceberg"

  # Iceberg + Glue Catalog Spark config. Must be passed as job-level
  # --conf arguments (not set programmatically in the scripts), since
  # these have to exist before Glue creates the SparkContext.
  iceberg_glue_catalog_conf = join(" ", [
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog",
    "--conf spark.sql.catalog.local.warehouse=${local.iceberg_warehouse_path}",
    "--conf spark.sql.catalog.local.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "--conf spark.sql.catalog.local.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    "--conf spark.sql.defaultCatalog=local",
  ])
}

resource "aws_glue_job" "kafka_to_iceberg" {
  name     = "${var.project_name}-${var.environment}-kafka-to-iceberg"
  role_arn = aws_iam_role.glue_execution.arn

  command {
    name            = "gluestreaming"
    script_location = "${local.glue_script_prefix}/kafka_to_iceberg.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  connections       = [aws_glue_connection.vpc.name]

  default_arguments = {
    "--job-language"                     = "python"
    "--warehouse_path"                   = local.iceberg_warehouse_path
    "--kafka_bootstrap_servers"          = aws_msk_serverless_cluster.main.bootstrap_brokers_sasl_iam
    "--datalake-formats"                 = "iceberg"
    "--extra-jars"                       = var.msk_iam_auth_jar_s3_path
    "--conf"                             = local.iceberg_glue_catalog_conf
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-kafka-to-iceberg"
  }
}

resource "aws_glue_job" "aggregate_data" {
  name     = "${var.project_name}-${var.environment}-aggregate-data"
  role_arn = aws_iam_role.glue_execution.arn

  command {
    name            = "gluestreaming"
    script_location = "${local.glue_script_prefix}/aggregate_data.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  connections       = [aws_glue_connection.vpc.name]

  default_arguments = {
    "--job-language"                     = "python"
    "--warehouse_path"                   = local.iceberg_warehouse_path
    "--airport_longlat_path"             = "${local.glue_reference_prefix}/airport_longlat.csv"
    "--aircraft_database_path"           = "${local.glue_reference_prefix}/aircraft-database-complete-2025-08.csv"
    "--datalake-formats"                 = "iceberg"
    "--additional-python-modules"        = "holidays"
    "--conf"                             = local.iceberg_glue_catalog_conf
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-aggregate-data"
  }
}
