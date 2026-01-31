variable "instance_type" {
  description = "EC2 instance type"
  type        = string
  default     = "m7i.2xlarge"
}

variable "ebs_volume_size" {
  description = "Size in GB for persistent EBS volume"
  type        = number
  default     = 500
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "vpc_id" {
  description = "VPC ID (optional, will use default VPC if not specified)"
  type        = string
  default     = ""
}

variable "subnet_id" {
  description = "Subnet ID (optional)"
  type        = string
  default     = ""
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed SSH access (default: VPC CIDR)"
  type        = list(string)
  default     = []
}

variable "enable_s3_access" {
  description = "Whether to grant S3 access to instance"
  type        = bool
  default     = true
}

variable "s3_bucket_name" {
  description = "S3 bucket to mount"
  type        = string
  default     = "data.sb"
}

variable "s3_mount_path" {
  description = "Path to mount S3 bucket"
  type        = string
  default     = "/data.sb"
}

variable "project_name" {
  description = "Name prefix for resources"
  type        = string
  default     = "rate-design-platform"
}

variable "allowed_iam_principals" {
  description = "Optional list of IAM principal ARNs allowed to access (for reference/documentation, actual access controlled by IAM policies)"
  type        = list(string)
  default     = []
}
