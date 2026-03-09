variable "workers" {
  description = "Map of worker name to instance type override (empty string = use default_instance_type)"
  type        = map(string)
  default     = {}
}

variable "default_instance_type" {
  description = "Default EC2 instance type for fleet workers"
  type        = string
  default     = "m7i.xlarge"
}

variable "snapshot_id" {
  description = "EBS snapshot ID to create worker data volumes from"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "project_name" {
  description = "Name prefix for fleet resources"
  type        = string
  default     = "rdp-fleet"
}

variable "dev_project_name" {
  description = "Name prefix used by dev infrastructure (for looking up existing IAM/SG)"
  type        = string
  default     = "rate-design-platform"
}

variable "s3_bucket_name" {
  description = "S3 bucket to mount on workers"
  type        = string
  default     = "data.sb"
}

variable "s3_mount_path" {
  description = "Path to mount S3 bucket"
  type        = string
  default     = "/data.sb"
}
