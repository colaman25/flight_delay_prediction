locals {
  # These names are also the Kafka topic names. publish_to_kafka/handler.py
  # derives the target topic from the triggering queue's name (via
  # eventSourceARN), so this list has to match the topics exactly.
  pipeline_topics = ["flight-data", "departure-schedule-data", "arrival-schedule-data"]
}

# =========================================================
# SQS queues (one per Kafka topic) + dead-letter queues
# =========================================================
# Queues are named to exactly match their Kafka topic (no project
# prefix), since publish_to_kafka/handler.py relies on that 1:1 match.

resource "aws_sqs_queue" "dlq" {
  for_each = toset(local.pipeline_topics)
  name     = "${each.value}-dlq"
}

resource "aws_sqs_queue" "queue" {
  for_each                   = toset(local.pipeline_topics)
  name                       = each.value
  visibility_timeout_seconds = 90 # >= publish_to_kafka's Lambda timeout

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq[each.value].arn
    maxReceiveCount     = 5
  })
}

# =========================================================
# ECR repositories for the three Lambda container images
# =========================================================

resource "aws_ecr_repository" "fetch_flight_data" {
  name         = "${var.project_name}-${var.environment}-fetch-flight-data"
  force_delete = true # images get rebuilt/pushed often during iteration
}

resource "aws_ecr_repository" "fetch_schedule_data" {
  name         = "${var.project_name}-${var.environment}-fetch-schedule-data"
  force_delete = true
}

resource "aws_ecr_repository" "publish_to_kafka" {
  name         = "${var.project_name}-${var.environment}-publish-to-kafka"
  force_delete = true
}

resource "aws_ecr_repository" "create_msk_topics" {
  name         = "${var.project_name}-${var.environment}-create-msk-topics"
  force_delete = true
}

# =========================================================
# IAM: fetch Lambdas (SQS send only, never touch MSK/VPC)
# =========================================================

resource "aws_iam_role" "lambda_fetch" {
  name = "${var.project_name}-${var.environment}-lambda-fetch"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_fetch_basic" {
  role       = aws_iam_role.lambda_fetch.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_fetch" {
  name = "${var.project_name}-${var.environment}-lambda-fetch-policy"
  role = aws_iam_role.lambda_fetch.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "SendToQueues"
      Effect   = "Allow"
      Action   = ["sqs:SendMessage", "sqs:SendMessageBatch"]
      Resource = [for t in local.pipeline_topics : aws_sqs_queue.queue[t].arn]
    }]
  })
}

# =========================================================
# IAM: publish_to_kafka Lambda (SQS receive + MSK write + VPC)
# =========================================================

resource "aws_iam_role" "lambda_publish" {
  name = "${var.project_name}-${var.environment}-lambda-publish"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_publish_basic" {
  role       = aws_iam_role.lambda_publish.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_publish_vpc" {
  role       = aws_iam_role.lambda_publish.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "lambda_publish" {
  name = "${var.project_name}-${var.environment}-lambda-publish-policy"
  role = aws_iam_role.lambda_publish.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ConsumeQueues"
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
        ]
        Resource = [for t in local.pipeline_topics : aws_sqs_queue.queue[t].arn]
      },
      {
        Sid      = "MskConnect"
        Effect   = "Allow"
        Action   = ["kafka-cluster:Connect"]
        Resource = aws_msk_serverless_cluster.main.arn
      },
      {
        Sid    = "MskTopicAccess"
        Effect = "Allow"
        Action = [
          "kafka-cluster:WriteData",
          "kafka-cluster:DescribeTopic",
        ]
        Resource = local.msk_topic_wildcard_arn
      },
    ]
  })
}

# =========================================================
# IAM: create_msk_topics Lambda (MSK topic admin + VPC only -- kept
# separate from lambda_publish, which deliberately can't create topics,
# same least-privilege split as lambda_fetch vs lambda_publish)
# =========================================================

resource "aws_iam_role" "lambda_create_topics" {
  name = "${var.project_name}-${var.environment}-lambda-create-topics"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_create_topics_basic" {
  role       = aws_iam_role.lambda_create_topics.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_create_topics_vpc" {
  role       = aws_iam_role.lambda_create_topics.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "lambda_create_topics" {
  name = "${var.project_name}-${var.environment}-lambda-create-topics-policy"
  role = aws_iam_role.lambda_create_topics.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "MskConnect"
        Effect   = "Allow"
        Action   = ["kafka-cluster:Connect"]
        Resource = aws_msk_serverless_cluster.main.arn
      },
      {
        Sid    = "MskTopicAccess"
        Effect = "Allow"
        Action = [
          "kafka-cluster:CreateTopic",
          "kafka-cluster:DescribeTopic",
        ]
        Resource = local.msk_topic_wildcard_arn
      },
    ]
  })
}

# =========================================================
# Lambda functions (container images from ECR)
# =========================================================
# NOTE: these reference ":latest" images that must already exist in ECR
# before `terraform apply` can create/update them -- see
# infra/deploy_lambda_images.sh and the sequencing note there.

resource "aws_lambda_function" "fetch_flight_data" {
  function_name = "${var.project_name}-${var.environment}-fetch-flight-data"
  role          = aws_iam_role.lambda_fetch.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.fetch_flight_data.repository_url}:latest"
  timeout       = 60
  memory_size   = 256

  environment {
    variables = {
      SQS_QUEUE_URL         = aws_sqs_queue.queue["flight-data"].url
      OPENSKY_CLIENT_ID     = var.opensky_client_id
      OPENSKY_CLIENT_SECRET = var.opensky_client_secret
    }
  }
}

resource "aws_lambda_function" "fetch_schedule_data" {
  function_name = "${var.project_name}-${var.environment}-fetch-schedule-data"
  role          = aws_iam_role.lambda_fetch.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.fetch_schedule_data.repository_url}:latest"
  timeout       = 120
  memory_size   = 256

  environment {
    variables = {
      DEPARTURE_QUEUE_URL = aws_sqs_queue.queue["departure-schedule-data"].url
      ARRIVAL_QUEUE_URL   = aws_sqs_queue.queue["arrival-schedule-data"].url
      FLIGHTAWARE_API     = var.flightaware_api_key
    }
  }
}

resource "aws_lambda_function" "publish_to_kafka" {
  function_name = "${var.project_name}-${var.environment}-publish-to-kafka"
  role          = aws_iam_role.lambda_publish.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.publish_to_kafka.repository_url}:latest"
  timeout       = 60
  memory_size   = 256

  vpc_config {
    subnet_ids         = aws_subnet.main[*].id
    security_group_ids = [aws_security_group.compute.id]
  }

  environment {
    variables = {
      # AWS_REGION is a reserved Lambda env var name (injected
      # automatically) -- not set here, handler.py reads it for free.
      KAFKA_BOOTSTRAP_SERVERS = aws_msk_serverless_cluster.main.bootstrap_brokers_sasl_iam
    }
  }
}

resource "aws_lambda_function" "create_msk_topics" {
  function_name = "${var.project_name}-${var.environment}-create-msk-topics"
  role          = aws_iam_role.lambda_create_topics.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.create_msk_topics.repository_url}:latest"
  timeout       = 60
  memory_size   = 256

  # VPC-attached to reach MSK, same as publish_to_kafka -- and same
  # reasoning: no internet access needed here since it only talks to
  # MSK, which is inside this VPC.
  vpc_config {
    subnet_ids         = aws_subnet.main[*].id
    security_group_ids = [aws_security_group.compute.id]
  }

  environment {
    variables = {
      # AWS_REGION is a reserved Lambda env var name (injected
      # automatically) -- not set here, handler.py reads it for free.
      KAFKA_BOOTSTRAP_SERVERS = aws_msk_serverless_cluster.main.bootstrap_brokers_sasl_iam
    }
  }
}

# =========================================================
# Wire each SQS queue to publish_to_kafka
# =========================================================

resource "aws_lambda_event_source_mapping" "publish_from_queue" {
  for_each         = toset(local.pipeline_topics)
  event_source_arn = aws_sqs_queue.queue[each.value].arn
  function_name    = aws_lambda_function.publish_to_kafka.arn
  batch_size       = 10

  # Only messages that actually fail to publish get retried/DLQ'd,
  # rather than the whole batch -- handler.py returns batchItemFailures
  # to make use of this.
  function_response_types = ["ReportBatchItemFailures"]
}

# =========================================================
# EventBridge Scheduler: recurring triggers for the fetch Lambdas
# =========================================================
# Matches the POLL_INTERVAL cadence from the original local scripts
# (900s / 1800s) -- note the local scripts never actually had a
# recurring schedule (Airflow's get_data DAG is schedule=None, manual
# trigger only), so this is genuinely new automatic execution, not a
# like-for-like replacement of an existing cron.

resource "aws_iam_role" "scheduler_invoke" {
  name = "${var.project_name}-${var.environment}-scheduler-invoke"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "scheduler_invoke" {
  name = "${var.project_name}-${var.environment}-scheduler-invoke-policy"
  role = aws_iam_role.scheduler_invoke.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "lambda:InvokeFunction"
      Resource = [
        aws_lambda_function.fetch_flight_data.arn,
        aws_lambda_function.fetch_schedule_data.arn,
      ]
    }]
  })
}

resource "aws_scheduler_schedule" "fetch_flight_data" {
  name                         = "${var.project_name}-${var.environment}-fetch-flight-data"
  schedule_expression          = "rate(15 minutes)"
  schedule_expression_timezone = "UTC"
  # Paused until create_msk_topics has actually been run and validated --
  # flip to "ENABLED" (or remove this line entirely, since ENABLED is the
  # default) and `terraform apply` to turn it back on.
  state = "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.fetch_flight_data.arn
    role_arn = aws_iam_role.scheduler_invoke.arn
  }
}

resource "aws_scheduler_schedule" "fetch_schedule_data" {
  name                         = "${var.project_name}-${var.environment}-fetch-schedule-data"
  schedule_expression          = "rate(30 minutes)"
  schedule_expression_timezone = "UTC"
  # Paused until create_msk_topics has actually been run and validated --
  # flip to "ENABLED" (or remove this line entirely, since ENABLED is the
  # default) and `terraform apply` to turn it back on.
  state = "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.fetch_schedule_data.arn
    role_arn = aws_iam_role.scheduler_invoke.arn
  }
}
