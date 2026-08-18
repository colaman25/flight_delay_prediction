# NEW code -- not a port of anything in proj_docker/. The local project's
# producers (get_flight_data.py / get_schedule_data.py) always talked to
# Kafka directly; on AWS that job is split out into this single, shared,
# VPC-attached Lambda, which is the one piece of this pipeline that
# genuinely needs both SQS access and MSK/VPC access.
#
# Triggered by three SQS queues (flight-data, departure-schedule-data,
# arrival-schedule-data -- see infra/lambda_pipeline.tf). The target Kafka
# topic is derived from the name of whichever queue triggered this
# invocation, since queue names match topic names 1:1 by design -- so one
# function body handles all three.
#
# Uses OAUTHBEARER + aws-msk-iam-sasl-signer-python for MSK IAM auth. This
# is deliberately different from the AWS_MSK_IAM mechanism used in
# glue_jobs/kafka_to_iceberg.py -- that's the Java client's convention
# (via the aws-msk-iam-auth jar); kafka-python (a pure-Python client) uses
# OAUTHBEARER with a token-provider callback instead.
import os

from kafka import KafkaProducer
from kafka.net.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
AWS_REGION = os.environ["AWS_REGION"]


class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
        return token


# Created once per execution environment and reused across warm
# invocations, rather than reconnecting to MSK on every single message.
_producer = None


def get_producer():
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            security_protocol="SASL_SSL",
            sasl_mechanism="OAUTHBEARER",
            sasl_oauth_token_provider=MSKTokenProvider(),
            value_serializer=lambda v: v.encode("utf-8"),
        )
    return _producer


def topic_from_event_source_arn(event_source_arn):
    # arn:aws:sqs:<region>:<account>:<queue-name> -- queue name IS the topic name
    return event_source_arn.split(":")[-1]


def lambda_handler(event, context):
    producer = get_producer()
    batch_item_failures = []

    for record in event["Records"]:
        topic = topic_from_event_source_arn(record["eventSourceARN"])
        try:
            producer.send(topic, value=record["body"])
        except Exception as e:
            print(f"[ERROR] Failed to publish to {topic}: {e}")
            batch_item_failures.append({"itemIdentifier": record["messageId"]})

    producer.flush()

    # Only the failed messages get retried/sent to the DLQ, not the whole
    # batch -- requires function_response_types = ["ReportBatchItemFailures"]
    # on the event source mapping (see infra/lambda_pipeline.tf).
    return {"batchItemFailures": batch_item_failures}
