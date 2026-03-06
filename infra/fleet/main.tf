terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# --- Data sources: AMI + existing dev infrastructure ---

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

data "aws_iam_instance_profile" "ec2_profile" {
  name = "${var.dev_project_name}-ec2-profile"
}

data "aws_security_group" "ec2_sg" {
  name = "${var.dev_project_name}-sg"
}

data "aws_subnets" "available" {
  filter {
    name   = "vpc-id"
    values = [data.aws_security_group.ec2_sg.vpc_id]
  }
}

data "aws_subnet" "first" {
  id = data.aws_subnets.available.ids[0]
}

locals {
  availability_zone = data.aws_subnet.first.availability_zone
  user_data = templatefile("${path.module}/../user-data.sh", {
    s3_bucket_name = var.s3_bucket_name
    s3_mount_path  = var.s3_mount_path
  })
}

# --- Fleet worker resources ---

resource "aws_instance" "worker" {
  for_each = var.workers

  ami                    = data.aws_ami.ubuntu.id
  instance_type          = each.value != "" ? each.value : var.default_instance_type
  availability_zone      = local.availability_zone
  vpc_security_group_ids = [data.aws_security_group.ec2_sg.id]
  subnet_id              = data.aws_subnet.first.id
  iam_instance_profile   = data.aws_iam_instance_profile.ec2_profile.name
  user_data              = local.user_data

  root_block_device {
    volume_type = "gp3"
    volume_size = 30
    encrypted   = true
  }

  tags = {
    Name      = "${var.project_name}-${each.key}"
    Project   = var.project_name
    Worker    = each.key
    ManagedBy = "terraform"
  }
}

resource "aws_ebs_volume" "worker" {
  for_each = var.workers

  availability_zone = local.availability_zone
  snapshot_id       = var.snapshot_id
  type              = "gp3"
  encrypted         = true

  tags = {
    Name      = "${var.project_name}-${each.key}-data"
    Project   = var.project_name
    Worker    = each.key
    ManagedBy = "terraform"
  }
}

resource "aws_volume_attachment" "worker" {
  for_each = var.workers

  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.worker[each.key].id
  instance_id = aws_instance.worker[each.key].id
}
