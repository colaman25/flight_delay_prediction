# Glue Data Catalog database for the Iceberg tables this pipeline
# writes/reads. Replaces the local Hadoop (directory-based) catalog,
# which isn't safe for concurrent writers on S3.
resource "aws_glue_catalog_database" "warehouse" {
  name        = replace("${var.project_name}_${var.environment}", "-", "_")
  description = "Iceberg catalog for the flight-analysis AWS migration"

  location_uri = "s3://${aws_s3_bucket.warehouse.bucket}/iceberg/"
}
