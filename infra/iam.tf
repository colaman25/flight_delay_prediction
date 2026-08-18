data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
}

# =========================================================
# Glue job execution role (kafka_to_iceberg, aggregate_data,
# predict_to_iceberg as Glue Streaming ETL; train_models as a
# Glue batch job)
# =========================================================

resource "aws_iam_role" "glue_execution" {
  name = "${var.project_name}-${var.environment}-glue-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_execution" {
  name = "${var.project_name}-${var.environment}-glue-execution-policy"
  role = aws_iam_role.glue_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "WarehouseBucketReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.warehouse.arn,
          "${aws_s3_bucket.warehouse.arn}/*",
        ]
      },
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchCreatePartition",
        ]
        Resource = [
          "arn:aws:glue:${local.region}:${local.account_id}:catalog",
          aws_glue_catalog_database.warehouse.arn,
          "${aws_glue_catalog_database.warehouse.arn}/*",
        ]
      },
      {
        Sid    = "MskConnect"
        Effect = "Allow"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeCluster",
          "kafka-cluster:AlterCluster",
        ]
        Resource = aws_msk_serverless_cluster.main.arn
      },
      {
        Sid    = "MskTopicAccess"
        Effect = "Allow"
        Action = [
          "kafka-cluster:ReadData",
          "kafka-cluster:WriteData",
          "kafka-cluster:DescribeTopic",
        ]
        Resource = local.msk_topic_wildcard_arn
      },
    ]
  })
}

# =========================================================
# MWAA execution role (orchestration, Phase 5). Created now so
# it's ready to attach when the MWAA environment is stood up;
# the dedicated DAGs bucket permissions get added in Phase 5.
# =========================================================

resource "aws_iam_role" "mwaa_execution" {
  name = "${var.project_name}-${var.environment}-mwaa-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = [
          "airflow.amazonaws.com",
          "airflow-env.amazonaws.com",
        ]
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "mwaa_execution" {
  name = "${var.project_name}-${var.environment}-mwaa-execution-policy"
  role = aws_iam_role.mwaa_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "WarehouseBucketRead"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.warehouse.arn,
          "${aws_s3_bucket.warehouse.arn}/*",
        ]
      },
      {
        Sid    = "Logging"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:CreateLogGroup",
          "logs:PutLogEvents",
          "logs:GetLogEvents",
          "logs:GetLogRecord",
          "logs:GetLogGroupFields",
          "logs:GetQueryResults",
        ]
        Resource = "arn:aws:logs:${local.region}:${local.account_id}:log-group:airflow-${var.project_name}-${var.environment}-*"
      },
      {
        Sid      = "CloudWatchMetrics"
        Effect   = "Allow"
        Action   = "cloudwatch:PutMetricData"
        Resource = "*"
      },
      {
        Sid    = "InvokeGlueAndEmr"
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun",
        ]
        Resource = "*"
      },
    ]
  })
}

# =========================================================
# Lambda execution role (prediction API, Phase 4)
# =========================================================

resource "aws_iam_role" "lambda_api" {
  name = "${var.project_name}-${var.environment}-lambda-api"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_api.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_api" {
  name = "${var.project_name}-${var.environment}-lambda-api-policy"
  role = aws_iam_role.lambda_api.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "WarehouseBucketRead"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.warehouse.arn,
          "${aws_s3_bucket.warehouse.arn}/*",
        ]
      },
      {
        Sid    = "GlueCatalogRead"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartitions",
        ]
        Resource = [
          "arn:aws:glue:${local.region}:${local.account_id}:catalog",
          aws_glue_catalog_database.warehouse.arn,
          "${aws_glue_catalog_database.warehouse.arn}/*",
        ]
      },
      {
        Sid    = "AthenaQuery"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
        ]
        Resource = "arn:aws:athena:${local.region}:${local.account_id}:workgroup/primary"
      },
    ]
  })
}
