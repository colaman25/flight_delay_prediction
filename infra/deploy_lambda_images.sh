#!/usr/bin/env bash
# Builds and pushes the four Lambda container images to their ECR
# repositories. Run this after the ECR repos exist (see the sequencing
# note below) and again any time lambda_jobs/* changes.
#
# Deliberately does NOT use `terraform output` for the region/repo URLs:
# this script is specifically meant to run in the gap *before* a full
# `terraform apply` (see below), and outputs aren't written to state
# until a full, non-targeted apply runs -- so relying on them here would
# make the script unusable for the one situation it exists for. It reads
# everything directly from AWS/the CLI config instead.
#
# Sequencing note: aws_lambda_function.image_uri is validated at
# creation/update time (unlike Glue's script_location, which Glue only
# checks when a job actually runs) -- so the FIRST apply needs the ECR
# repos to exist and have an image pushed *before* the Lambda function
# resources themselves can be created. In practice: apply once with
# -target on the four aws_ecr_repository resources, run this script,
# then a normal `terraform apply` for everything else.
set -euo pipefail

cd "$(dirname "$0")"

REPO_ROOT="$(cd .. && pwd)"

# Must match project_name/environment in terraform.tfvars.
PROJECT_NAME="flight-analysis"
ENVIRONMENT="aws-migration"

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=$(aws configure get region)

get_repo_uri () {
  aws ecr describe-repositories --repository-names "$1" --query "repositories[0].repositoryUri" --output text
}

FETCH_FLIGHT_REPO=$(get_repo_uri "${PROJECT_NAME}-${ENVIRONMENT}-fetch-flight-data")
FETCH_SCHEDULE_REPO=$(get_repo_uri "${PROJECT_NAME}-${ENVIRONMENT}-fetch-schedule-data")
PUBLISH_REPO=$(get_repo_uri "${PROJECT_NAME}-${ENVIRONMENT}-publish-to-kafka")
CREATE_TOPICS_REPO=$(get_repo_uri "${PROJECT_NAME}-${ENVIRONMENT}-create-msk-topics")

echo "Logging in to ECR..."
aws ecr get-login-password --region "${REGION}" \
  | docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

build_and_push () {
  local dir="$1"
  local repo="$2"
  echo "Building ${dir} -> ${repo}:latest"
  docker build -t "${repo}:latest" "${REPO_ROOT}/lambda_jobs/${dir}"
  docker push "${repo}:latest"
}

build_and_push "fetch_flight_data"    "${FETCH_FLIGHT_REPO}"
build_and_push "fetch_schedule_data"  "${FETCH_SCHEDULE_REPO}"
build_and_push "publish_to_kafka"     "${PUBLISH_REPO}"
build_and_push "create_msk_topics"    "${CREATE_TOPICS_REPO}"

echo "Done. All four Lambda images are pushed."
echo "If the Lambda functions already exist, update them to pick up the new image:"
echo "  aws lambda update-function-code --function-name <name> --image-uri <repo>:latest"
