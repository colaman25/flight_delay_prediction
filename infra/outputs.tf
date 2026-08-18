output "region" {
  value = var.aws_region
}

output "vpc_id" {
  value = aws_vpc.main.id
}

output "subnet_ids" {
  description = "Public subnets for MSK brokers, Glue/EMR ENIs, and the publish_to_kafka Lambda (the fetch Lambdas and the API are not VPC-attached)"
  value       = aws_subnet.main[*].id
}

output "msk_security_group_id" {
  value = aws_security_group.msk.id
}

output "compute_security_group_id" {
  description = "Attach this to Glue connections / EMR nodes, and to the publish_to_kafka Lambda"
  value       = aws_security_group.compute.id
}

output "msk_cluster_arn" {
  value = aws_msk_serverless_cluster.main.arn
}

output "msk_bootstrap_brokers" {
  description = "IAM-auth bootstrap broker endpoint for Phase 2 Glue jobs and the optional local KAFKA_BOOTSTRAP_SERVERS toggle"
  value       = aws_msk_serverless_cluster.main.bootstrap_brokers_sasl_iam
}

output "warehouse_bucket_name" {
  value = aws_s3_bucket.warehouse.bucket
}

output "warehouse_bucket_arn" {
  value = aws_s3_bucket.warehouse.arn
}

output "glue_database_name" {
  value = aws_glue_catalog_database.warehouse.name
}

output "glue_execution_role_arn" {
  value = aws_iam_role.glue_execution.arn
}

output "mwaa_execution_role_arn" {
  value = aws_iam_role.mwaa_execution.arn
}

output "lambda_api_role_arn" {
  value = aws_iam_role.lambda_api.arn
}

output "fetch_flight_data_ecr_repository_url" {
  value = aws_ecr_repository.fetch_flight_data.repository_url
}

output "fetch_schedule_data_ecr_repository_url" {
  value = aws_ecr_repository.fetch_schedule_data.repository_url
}

output "publish_to_kafka_ecr_repository_url" {
  value = aws_ecr_repository.publish_to_kafka.repository_url
}
