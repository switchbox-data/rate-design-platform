output "worker_instances" {
  description = "Map of worker name to instance ID"
  value       = { for k, v in aws_instance.worker : k => v.id }
}

output "worker_ips" {
  description = "Map of worker name to private IP"
  value       = { for k, v in aws_instance.worker : k => v.private_ip }
}

output "snapshot_id" {
  description = "EBS snapshot ID used for this fleet"
  value       = var.snapshot_id
}
