#!/bin/bash
set -euo pipefail

# Log all output
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

echo "Starting user-data script..."

# Update system packages
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get upgrade -y

# Install system dependencies
apt-get install -y \
    python3.11 \
    python3.11-dev \
    python3-pip \
    git \
    build-essential \
    curl \
    unzip \
    s3fs \
    e2fsprogs \
    amazon-ssm-agent \
    awscli

# Install uv system-wide
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.cargo/bin:$PATH"
# Install to /usr/local/bin so it's available system-wide
cp "$HOME/.cargo/bin/uv" /usr/local/bin/uv 2>/dev/null || true
chmod +x /usr/local/bin/uv
# Also add to system PATH for all users (backup)
echo 'export PATH="$HOME/.cargo/bin:/usr/local/bin:$PATH"' >> /etc/profile.d/uv.sh

# Find the EBS volume device
# The volume is attached as /dev/sdf, but on newer instances it might be /dev/nvme1n1
EBS_DEVICE=""
if [ -b /dev/nvme1n1 ]; then
    EBS_DEVICE="/dev/nvme1n1"
elif [ -b /dev/sdf ]; then
    EBS_DEVICE="/dev/sdf"
else
    echo "ERROR: Could not find EBS volume device"
    exit 1
fi

echo "Found EBS device: $EBS_DEVICE"

# Check if volume needs formatting
if ! blkid $EBS_DEVICE > /dev/null 2>&1; then
    echo "Formatting EBS volume..."
    mkfs.ext4 -F $EBS_DEVICE
fi

# Create mount point
mkdir -p /data

# Mount the volume
mount $EBS_DEVICE /data

# Check if filesystem needs resizing (if volume was increased)
VOLUME_SIZE=$(blockdev --getsize64 $EBS_DEVICE)
FILESYSTEM_SIZE=$(df -B1 /data | tail -1 | awk '{print $2}')
if [ $VOLUME_SIZE -gt $FILESYSTEM_SIZE ]; then
    echo "Resizing filesystem to match volume size..."
    resize2fs $EBS_DEVICE
fi

# Add to fstab for persistence
EBS_UUID=$(blkid -s UUID -o value $EBS_DEVICE)
if ! grep -q "$EBS_UUID" /etc/fstab; then
    echo "UUID=$EBS_UUID /data ext4 defaults,nofail 0 2" >> /etc/fstab
fi

# Create directory structure on EBS volume
mkdir -p /data/home
mkdir -p /data/shared
chmod 755 /data
chmod 755 /data/home
chmod 777 /data/shared  # Shared directory for all users

# Set up S3 mount
mkdir -p ${s3_mount_path}
chmod 755 ${s3_mount_path}

# Enable user_allow_other in fuse.conf (required for allow_other mount option)
if ! grep -q "^user_allow_other" /etc/fuse.conf; then
    sed -i 's/#user_allow_other/user_allow_other/' /etc/fuse.conf || echo "user_allow_other" >> /etc/fuse.conf
fi

# Mount S3 bucket using IAM instance profile
# s3fs automatically uses IAM role when no credentials are specified
# Note: use_path_request_style is required for bucket names with dots (like data.sb)
echo "${s3_bucket_name} ${s3_mount_path} fuse.s3fs _netdev,allow_other,use_cache=/tmp/s3fs-cache,iam_role=auto,umask=0002,use_path_request_style,endpoint=us-west-2,url=https://s3.us-west-2.amazonaws.com 0 0" >> /etc/fstab

# Try to mount S3 (may fail if network not ready, will be mounted on boot)
# Wait a bit for network to be ready
sleep 10
# Mount in background since it can take time to initialize
if ! mountpoint -q ${s3_mount_path}; then
    nohup mount ${s3_mount_path} > /tmp/s3fs-mount.log 2>&1 &
    echo "S3 mount initiated in background (check /tmp/s3fs-mount.log for status)"
fi

# Create cache directory for s3fs
mkdir -p /tmp/s3fs-cache
chmod 1777 /tmp/s3fs-cache

# Start and enable SSM agent (for AWS Systems Manager Session Manager)
systemctl enable amazon-ssm-agent
systemctl start amazon-ssm-agent

echo "User-data script completed successfully!"
