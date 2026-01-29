output "instance_id" {
  description = "EC2 instance ID"
  value       = aws_instance.main.id
}

output "instance_public_ip" {
  description = "Public IP address of the EC2 instance"
  value       = aws_instance.main.public_ip
}

output "instance_private_ip" {
  description = "Private IP address of the EC2 instance"
  value       = aws_instance.main.private_ip
}

output "ebs_volume_id" {
  description = "EBS volume ID for persistent data"
  value       = aws_ebs_volume.data.id
}

output "availability_zone" {
  description = "Availability zone of the instance"
  value       = aws_instance.main.availability_zone
}

output "connection_info" {
  description = "Connection information"
  value = {
    instance_id     = aws_instance.main.id
    public_ip      = aws_instance.main.public_ip
    private_ip     = aws_instance.main.private_ip
    availability_zone = aws_instance.main.availability_zone
  }
}
