# MSK Serverless — no broker sizing/scaling to manage, bills only for
# actual partition-hours/storage/data transfer rather than per-broker-hour
# whether idle or not (unlike provisioned MSK). IAM authentication is the
# only auth mode it supports, which is also the only one this project's
# IAM policies (see iam.tf) are written for.
resource "aws_msk_serverless_cluster" "main" {
  cluster_name = "${var.project_name}-${var.environment}"

  vpc_config {
    subnet_ids         = aws_subnet.main[*].id
    security_group_ids = [aws_security_group.msk.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-msk"
  }
}

# MSK's IAM access control has separate resource types for the cluster
# itself (arn:...:cluster/name/uuid) vs. individual topics
# (arn:...:topic/name/uuid/topic-name). Actions like Connect/
# DescribeCluster/AlterCluster are authorized against the cluster ARN;
# topic-level actions (CreateTopic/ReadData/WriteData/DescribeTopic) are
# authorized against a topic ARN, even when you mean "all topics" via a
# wildcard -- the cluster ARN alone does not cover them, no matter which
# actions are listed against it.
locals {
  msk_topic_wildcard_arn = "${replace(aws_msk_serverless_cluster.main.arn, ":cluster/", ":topic/")}/*"
}
