#!/usr/bin/env bash
# Uploads the Glue job scripts, reference data, and the aws-msk-iam-auth
# jar to S3. Run this after `terraform apply` (so the bucket exists) and
# again any time glue_jobs/*.py changes — these are deliberately NOT
# Terraform-managed, since they'd otherwise change on every edit during
# iteration.
set -euo pipefail

cd "$(dirname "$0")"

# Read directly from tfvars' value rather than `terraform output` -- the
# same fragility bug we hit in deploy_lambda_images.sh applies here too
# (outputs aren't populated until a full, non-targeted apply has run).
BUCKET="flight-delay-predictions-975050346356"
REPO_ROOT="$(cd .. && pwd)"

# Bump this if you want a newer aws-msk-iam-auth release — check
# https://repo1.maven.org/maven2/software/amazon/msk/aws-msk-iam-auth/
# for available versions.
MSK_IAM_AUTH_VERSION="2.2.0"
MSK_IAM_AUTH_JAR="aws-msk-iam-auth-${MSK_IAM_AUTH_VERSION}-all.jar"
MSK_IAM_AUTH_URL="https://repo1.maven.org/maven2/software/amazon/msk/aws-msk-iam-auth/${MSK_IAM_AUTH_VERSION}/${MSK_IAM_AUTH_JAR}"
MSK_IAM_AUTH_S3_KEY="glue-jars/${MSK_IAM_AUTH_JAR}"

echo "Deploying to s3://${BUCKET}"

aws s3 cp "${REPO_ROOT}/glue_jobs/kafka_to_iceberg.py"   "s3://${BUCKET}/glue-scripts/kafka_to_iceberg.py"
aws s3 cp "${REPO_ROOT}/glue_jobs/aggregate_data.py"     "s3://${BUCKET}/glue-scripts/aggregate_data.py"

aws s3 cp "${REPO_ROOT}/airport_longlat.csv"                    "s3://${BUCKET}/reference-data/airport_longlat.csv"
aws s3 cp "${REPO_ROOT}/aircraft-database-complete-2025-08.csv" "s3://${BUCKET}/reference-data/aircraft-database-complete-2025-08.csv"

# The jar itself never changes once pinned, so skip re-downloading/
# re-uploading if it's already sitting in S3.
if aws s3api head-object --bucket "${BUCKET}" --key "${MSK_IAM_AUTH_S3_KEY}" >/dev/null 2>&1; then
  echo "aws-msk-iam-auth ${MSK_IAM_AUTH_VERSION} already present in S3, skipping."
else
  echo "Fetching aws-msk-iam-auth ${MSK_IAM_AUTH_VERSION} from Maven Central..."
  TMP_JAR="$(mktemp -d)/${MSK_IAM_AUTH_JAR}"
  curl -fLo "${TMP_JAR}" "${MSK_IAM_AUTH_URL}"
  aws s3 cp "${TMP_JAR}" "s3://${BUCKET}/${MSK_IAM_AUTH_S3_KEY}"
  rm -rf "$(dirname "${TMP_JAR}")"
fi

echo "Done. Scripts, reference data, and the MSK IAM auth jar are up to date."
echo
echo "msk_iam_auth_jar_s3_path should be set to:"
echo "  s3://${BUCKET}/${MSK_IAM_AUTH_S3_KEY}"
