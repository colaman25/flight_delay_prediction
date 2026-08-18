# =========================================================
# VPC
# =========================================================

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${var.project_name}-${var.environment}-vpc"
  }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.project_name}-${var.environment}-igw"
  }
}

# =========================================================
# Subnets — everything (MSK, Glue/EMR ENIs) runs in public
# subnets, routed directly to the Internet Gateway. No NAT
# gateway. Security groups (below) are what actually restrict
# access — being in a public subnet doesn't by itself expose
# anything; that would additionally require an inbound rule
# with a 0.0.0.0/0 source, which none of these have.
# =========================================================

resource "aws_subnet" "main" {
  count                   = length(var.azs)
  vpc_id                  = aws_vpc.main.id
  cidr_block              = var.subnet_cidrs[count.index]
  availability_zone       = var.azs[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.project_name}-${var.environment}-${var.azs[count.index]}"
  }
}

resource "aws_route_table" "main" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-rt"
  }
}

resource "aws_route_table_association" "main" {
  count          = length(aws_subnet.main)
  subnet_id      = aws_subnet.main[count.index].id
  route_table_id = aws_route_table.main.id
}

# Any Glue job using a VPC connection (aws_glue_connection.vpc) must be
# able to reach S3 from within that VPC -- not for the job's own logic,
# but because Glue's own internal machinery needs S3 access (to fetch
# the job script itself, write logs, etc.), and Glue validates this
# before the job even starts. A Gateway endpoint satisfies that
# requirement for free (unlike a NAT gateway or an Interface endpoint) --
# it just adds a route, associated here with the one shared route table
# that covers all three subnets.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.main.id]
}

# =========================================================
# Security groups
# =========================================================

# MSK brokers: plaintext/TLS/IAM-auth ports, reachable only from
# other resources in this VPC (the compute security group below).
resource "aws_security_group" "msk" {
  name        = "${var.project_name}-${var.environment}-msk"
  description = "MSK broker access for the flight-analysis migration"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Kafka plaintext/TLS/IAM broker ports from within the VPC"
    from_port   = 9092
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  ingress {
    description = "Zookeeper (if used by client tooling) from within the VPC"
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-msk-sg"
  }
}

# Shared by Glue job connections / EMR nodes: self-referencing so
# cluster members can talk to each other, plus MSK access.
resource "aws_security_group" "compute" {
  name        = "${var.project_name}-${var.environment}-compute"
  description = "Glue/EMR job traffic for the flight-analysis migration"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-compute-sg"
  }
}

resource "aws_vpc_security_group_ingress_rule" "compute_self" {
  security_group_id            = aws_security_group.compute.id
  description                  = "Allow all traffic among Glue/EMR cluster members"
  referenced_security_group_id = aws_security_group.compute.id
  ip_protocol                  = "-1"
}

resource "aws_vpc_security_group_ingress_rule" "msk_from_compute" {
  security_group_id            = aws_security_group.msk.id
  description                  = "Allow Glue/EMR jobs to reach MSK brokers"
  referenced_security_group_id = aws_security_group.compute.id
  from_port                    = 9092
  to_port                      = 9098
  ip_protocol                  = "tcp"
}
