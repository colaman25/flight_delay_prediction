variable "aws_region" {
  description = "AWS region for the flight-analysis AWS migration"
  type        = string
  default     = "eu-west-2"
}

variable "project_name" {
  description = "Short name used to prefix and tag resources"
  type        = string
  default     = "flight-analysis"
}

variable "environment" {
  description = "Name of this deployment environment (kept distinct from any pre-existing AWS resources from local dev)"
  type        = string
  default     = "aws-migration"
}

variable "data_bucket_name" {
  description = <<-EOT
    Name of the S3 bucket used as the Iceberg warehouse for this migration.
    Must be globally unique. Deliberately a separate bucket from whatever
    bucket local Docker dev already points at, to avoid path collisions
    between local ad-hoc testing and this pipeline.
  EOT
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block for the migration VPC"
  type        = string
  default     = "10.42.0.0/16"
}

variable "azs" {
  description = "Availability zones to spread subnets across (MSK requires at least 2)"
  type        = list(string)
  default     = ["eu-west-2a", "eu-west-2b", "eu-west-2c"]
}

variable "subnet_cidrs" {
  description = "CIDR blocks (one per AZ) for the subnets that MSK and Glue/EMR ENIs live in. All public — routed directly to the Internet Gateway, no NAT gateway."
  type        = list(string)
  default     = ["10.42.1.0/24", "10.42.2.0/24", "10.42.3.0/24"]
}

variable "msk_iam_auth_jar_s3_path" {
  description = <<-EOT
    S3 path to the aws-msk-iam-auth jar (e.g. s3://<bucket>/glue-jars/aws-msk-iam-auth-2.x.x-all.jar),
    used by the kafka_to_iceberg Glue job to authenticate to MSK via IAM.
    Unlike Iceberg support (handled by --datalake-formats), Glue has no
    built-in shortcut for this — it must be uploaded to S3 first.
  EOT
  type        = string
}

variable "opensky_client_id" {
  description = "OpenSky API client ID, used by the fetch-flight-data Lambda"
  type        = string
  sensitive   = true
}

variable "opensky_client_secret" {
  description = "OpenSky API client secret, used by the fetch-flight-data Lambda"
  type        = string
  sensitive   = true
}

variable "flightaware_api_key" {
  description = "FlightAware AeroAPI key, used by the fetch-schedule-data Lambda"
  type        = string
  sensitive   = true
}
