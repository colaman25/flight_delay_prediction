# Lambda port of the now-removed glue_jobs/create_msk_topics.py -- moved
# off Glue Python Shell because --additional-python-modules does a live
# `pip install` from PyPI at job-start time, inside the VPC-restricted
# execution environment, which has no route to pypi.org (confirmed: the
# job hung for 5+ minutes retrying a connection to pypi.org before it
# would have eventually failed). A Lambda container image installs its
# dependencies during `docker build` instead -- on a machine with normal
# internet access -- so nothing needs to be fetched at runtime. Same
# reasoning, same dependency set, as lambda_jobs/publish_to_kafka.
#
# Manually invoked (`aws lambda invoke`), not on any schedule -- topic
# creation is a one-time/rare setup action, not a recurring one.
import os

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.net.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
AWS_REGION = os.environ["AWS_REGION"]  # reserved Lambda env var, injected automatically

# Matches proj_docker/kafka_topic_init/create_topics.py's topic list.
# replication_factor=3 (not the local script's 1) to match MSK
# Serverless's 3-AZ-managed replication.
TOPICS = [
    NewTopic(name="flight-data", num_partitions=1, replication_factor=3),
    NewTopic(name="departure-schedule-data", num_partitions=1, replication_factor=3),
    NewTopic(name="arrival-schedule-data", num_partitions=1, replication_factor=3),
    NewTopic(name="prediction-results", num_partitions=1, replication_factor=3),
]


class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
        return token


def lambda_handler(event, context):
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=MSKTokenProvider(),
        client_id="create-msk-topics",
    )

    results = {}
    for topic in TOPICS:
        try:
            admin.create_topics([topic])
            print(f"Topic '{topic.name}' created.")
            results[topic.name] = "created"
        except TopicAlreadyExistsError:
            print(f"Topic '{topic.name}' already exists.")
            results[topic.name] = "already_exists"

    admin.close()
    return results
