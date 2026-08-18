# flight-analysis AWS migration infrastructure

Terraform for the AWS side of the flight-analysis migration: VPC/networking,
MSK (Kafka), Glue (streaming ETL), S3 + Glue Data Catalog (the Iceberg
warehouse), IAM, and a Lambda/SQS pipeline that replaces the local
`get_flight_data`/`get_schedule_data` producers (and includes a one-off
`create_msk_topics` Lambda for topic setup). This runs alongside — not
instead of — the local Docker Compose stack in `proj_docker/`, which is
untouched by any of this.

## What this builds

| Area | Resources |
|---|---|
| Networking | VPC, Internet Gateway, 3 public subnets, 1 route table, 2 security groups (`msk`, `compute`), 1 S3 Gateway VPC endpoint (free — Glue's VPC-connected jobs need S3 reachability even with no NAT gateway) |
| Storage & Catalog | S3 bucket (Iceberg warehouse), Glue Data Catalog database |
| IAM | 7 roles: `glue_execution`, `mwaa_execution` (unused so far), `lambda_api` (unused so far), `lambda_fetch`, `lambda_publish`, `lambda_create_topics`, `scheduler_invoke` |
| MSK | 1 Serverless cluster (IAM auth only) |
| Glue | 1 VPC connection, 2 jobs — `kafka_to_iceberg` / `aggregate_data` (Streaming ETL) |
| Lambda/SQS/EventBridge | 3 SQS queues + 3 DLQs, 4 ECR repos, 4 Lambda functions (`fetch_flight_data`, `fetch_schedule_data` outside the VPC; `publish_to_kafka` and `create_msk_topics` inside it), 3 event source mappings, 2 EventBridge schedules |

Two scripts outside Terraform's management (deliberately — see [Why some
things aren't Terraform-managed](#why-some-things-arent-terraform-managed)):
`deploy_lambda_images.sh` and `deploy_glue_scripts.sh`.

## Prerequisites

- AWS CLI configured with credentials for the target account (`aws sts
  get-caller-identity` should work)
- Terraform >= 1.5
- Docker (for building the four Lambda images)

## First-time setup

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars
```

Fill in `terraform.tfvars`:
- `data_bucket_name` — must be globally unique across all of AWS. A
  reliable pattern: `<name>-<your-account-id>` (get the account ID via
  `aws sts get-caller-identity --query Account --output text`).
- `msk_iam_auth_jar_s3_path` — the S3 path `deploy_glue_scripts.sh` will
  upload the `aws-msk-iam-auth` jar to (same bucket, `glue-jars/` prefix).
- `opensky_client_id`, `opensky_client_secret`, `flightaware_api_key` —
  same credentials already used by the local Docker setup
  (`proj_docker/.env`).

`terraform.tfvars` is gitignored — never commit it.

## Deployment sequence

Run from a completely clean slate (nothing yet exists in AWS). Order
matters for steps 1–2; everything after that is just working through the
pipeline layer by layer.

```bash
cd infra
```

**1. Create the ECR repos first** — the one hard ordering constraint in
this whole sequence. `aws_lambda_function.image_uri` is validated at
creation time (unlike Glue's `script_location`, which Glue only checks
when a job actually runs), so the repos have to exist and have images in
them *before* the Lambda function resources can be created.

```powershell
terraform apply -target="aws_ecr_repository.fetch_flight_data" -target="aws_ecr_repository.fetch_schedule_data" -target="aws_ecr_repository.publish_to_kafka" -target="aws_ecr_repository.create_msk_topics"
```

(Quote the `-target` values — plain PowerShell has been observed
mis-parsing the unquoted `resource_type.resource_name` form.)

**2. Build and push the four Lambda images:**

```bash
./deploy_lambda_images.sh
```

**3. Full apply** — creates everything else: VPC/subnets/security groups,
S3 bucket, Glue Catalog database, IAM roles, the MSK Serverless cluster,
the Glue VPC connection, both Glue jobs, the SQS queues + DLQs, the four
Lambda functions (this time succeeding, since their images already
exist), the event source mappings, and the two EventBridge schedules
(created `DISABLED` — see [Enabling the fetch
pipeline](#enabling-the-fetch-pipeline)).

```bash
terraform apply
```

Expect this to take a while — MSK Serverless provisioning alone typically
takes several minutes, and Terraform blocks until it's ready.

**4. Upload the Glue scripts, reference CSVs, and the MSK IAM auth jar:**

```bash
./deploy_glue_scripts.sh
```

**5. Create the Kafka topics on MSK** — invoke the `create_msk_topics`
Lambda directly (it's a one-off, not wired to a schedule or trigger):

```bash
aws lambda invoke --function-name flight-analysis-aws-migration-create-msk-topics /tmp/create_topics_out.json
cat /tmp/create_topics_out.json
```

Check the output — it reports `"created"` or `"already_exists"` per topic.
If it errors, check the function's CloudWatch log group
(`/aws/lambda/flight-analysis-aws-migration-create-msk-topics`).

**6. Validate the fetch → SQS → publish → MSK path manually**, before
turning the schedules on — invoke each fetch Lambda once directly rather
than waiting for (or enabling) the automatic schedule:

```bash
aws lambda invoke --function-name flight-analysis-aws-migration-fetch-flight-data /tmp/out1.json
aws lambda invoke --function-name flight-analysis-aws-migration-fetch-schedule-data /tmp/out2.json
```

Then check the SQS queues and their DLQs — the DLQs should stay at 0 now
that topics actually exist. (If you skip step 5, this is exactly the
failure mode that dead-letters everything — confirmed the hard way once
already.)

**7. Validate the streaming Glue jobs.** These run continuously (not
one-shot like `create_msk_topics`), and bill DPU-hours for as long as
they're running:

```bash
aws glue start-job-run --job-name flight-analysis-aws-migration-kafka-to-iceberg
aws glue start-job-run --job-name flight-analysis-aws-migration-aggregate-data
```

## Enabling the fetch pipeline

The two EventBridge schedules are created `DISABLED` by default, so
nothing runs automatically until you decide it's ready. Once step 6 above
has been validated:

```hcl
# infra/lambda_pipeline.tf
resource "aws_scheduler_schedule" "fetch_flight_data" {
  ...
  # state = "DISABLED"   <- remove or flip to "ENABLED"
}
```

Then `terraform apply`. To pause them again later, flip back to
`"DISABLED"` and apply — don't toggle this via `aws scheduler
update-schedule` directly, since that creates drift that a future `apply`
would silently revert.

## Redeploying after code changes

- Changed `glue_jobs/*.py`? Re-run `./deploy_glue_scripts.sh`. Glue jobs
  pick up the new script on their *next* run — no `terraform apply`
  needed.
- Changed `lambda_jobs/*/handler.py`? Re-run `./deploy_lambda_images.sh`,
  then update each function to the new image:
  ```bash
  aws lambda update-function-code --function-name flight-analysis-aws-migration-<name> --image-uri <repo-url>:latest
  ```

## Why some things aren't Terraform-managed

`deploy_lambda_images.sh` and `deploy_glue_scripts.sh` intentionally sit
outside Terraform. Script/image content changes far more often than the
surrounding infrastructure does — folding "rebuild and push my code" into
the same `apply` that also plans changes to IAM/VPC/MSK adds friction and
noise to something that should be a fast, frequent action. Same reasoning
applies to Kafka topics, which are also deliberately not
Terraform-managed (no community Kafka provider dependency, no direct
network reachability requirement from wherever `terraform apply` runs).

Both scripts deliberately avoid `terraform output` for values like the
region or S3 bucket name — outputs aren't populated until a full,
non-targeted `apply` has run, but these scripts are specifically meant to
run in the gap *before* one (see step 1–2 above). They query AWS directly
instead (`aws configure get region`, `aws ecr describe-repositories`,
etc.), so they work regardless of what's been applied so far.

## Teardown

```bash
terraform destroy
```

Things that are already handled so this doesn't need manual cleanup:

- **ECR repos** have `force_delete = true` — they'll delete along with
  any images in them, no "repo not empty" error.
- **SQS queues** delete regardless of message content (including
  whatever's sitting in the DLQs).
- **MSK Serverless** deletes cluster and topics together in one shot —
  no "topics must be empty first" requirement like S3 has for objects.

Things worth knowing about, not necessarily requiring action:

- **The S3 bucket** will fail to delete if it has any objects in it
  (compounded by versioning — old versions and delete markers count too,
  even if nothing "visible" remains). Empty it via the console ("Empty
  bucket") or `aws s3 rm --recursive` (versions need a separate
  `list-object-versions`/`delete-objects` pass) if `destroy` fails on it.
- **Stop any active Glue job runs first** if `kafka_to_iceberg` or
  `aggregate_data` are actively streaming — deleting the job *definition*
  doesn't necessarily stop an in-flight *run*, which can keep billing
  DPU-hours outside Terraform's visibility. `aws glue batch-stop-job-run`
  first.
- **MSK's auto-created VPC Interface Endpoint** can lag behind the
  cluster's own deletion by several minutes, which can transiently block
  subnet/security-group deletion. If `destroy` seems stuck on those
  specifically, check `aws ec2 describe-vpc-endpoints` for one in
  `"deleting"` state tagged `AWSMSKManaged: true` — this is normal
  latency, not a stuck resource; just wait.

## Cost notes

Everything in Phase 0 (networking, S3, Glue Catalog, IAM) is free. Beyond
that:

- **MSK Serverless** is the first real recurring-cost resource — bills
  per partition-hour/storage/data-transfer rather than a flat per-broker
  charge, so it stays close to $0 at low/no traffic.
- **Glue jobs** only bill DPU-hours while actually *running* — the
  streaming jobs (`kafka_to_iceberg`, `aggregate_data`) accrue cost for as
  long as you leave them running.
- **Lambda, SQS, EventBridge Scheduler** are all effectively free at this
  project's traffic volume (well within AWS's free tiers); `create_msk_topics`
  runs for a few seconds, once.
- **ECR storage** adds a trivially small (well under $0.20/month) line
  item for the four Lambda images.

Nothing here auto-starts a Glue job — the definitions cost nothing until
explicitly run via `start-job-run`.
