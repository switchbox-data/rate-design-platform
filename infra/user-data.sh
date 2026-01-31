#!/bin/bash
set -euo pipefail

# Log all output
exec > >(tee /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

echo "Starting user-data script..."

# Update system packages
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get upgrade -y

# Resize root filesystem if volume was increased (runs on every boot)
# Get root device (could be /dev/sda1, /dev/nvme0n1p1, etc.)
ROOT_DEVICE=$(findmnt -n -o SOURCE / | sed 's/p[0-9]*$//' || echo "")
if [ -n "$ROOT_DEVICE" ] && [ -b "$ROOT_DEVICE" ]; then
  ROOT_VOLUME_SIZE=$(blockdev --getsize64 "$ROOT_DEVICE" 2>/dev/null || echo "0")
  ROOT_FS_SIZE=$(df -B1 / | tail -1 | awk '{print $2}')
  if [ "$ROOT_VOLUME_SIZE" -gt "$ROOT_FS_SIZE" ] && [ "$ROOT_VOLUME_SIZE" -gt 0 ]; then
    echo "Root volume size ($ROOT_VOLUME_SIZE) is larger than filesystem ($ROOT_FS_SIZE), resizing..."
    resize2fs "$ROOT_DEVICE" 2>&1 || echo "Resize may have failed, but continuing..."
  fi
fi

# Add deadsnakes PPA for Python 3.13
apt-get install -y software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update

# Install system dependencies
apt-get install -y \
  python3.13 \
  python3.13-dev \
  python3-pip \
  git \
  build-essential \
  curl \
  unzip \
  s3fs \
  e2fsprogs \
  awscli \
  gh \
  zsh

# Install SSM agent (not in default Ubuntu repos, use snap)
snap install amazon-ssm-agent --classic || true
# Or download from AWS if snap fails
if ! command -v amazon-ssm-agent &>/dev/null; then
  curl -o /tmp/amazon-ssm-agent.deb https://s3.amazonaws.com/ec2-downloads-windows/SSMAgent/latest/debian_amd64/amazon-ssm-agent.deb
  dpkg -i /tmp/amazon-ssm-agent.deb || apt-get install -f -y
  rm /tmp/amazon-ssm-agent.deb
fi

# Install uv system-wide
# Set HOME if not set (cloud-init runs as root without HOME set)
# Note: $$ escapes $ for Terraform templatefile
export HOME="$${HOME:-/root}"
curl -LsSf https://astral.sh/uv/install.sh | sh

# Find where uv was installed (installer uses ~/.cargo/bin or ~/.local/bin)
UV_BIN=""
for path in "$HOME/.cargo/bin/uv" "$HOME/.local/bin/uv" "/root/.cargo/bin/uv" "/root/.local/bin/uv"; do
  if [ -f "$path" ]; then
    UV_BIN="$path"
    break
  fi
done

if [ -z "$UV_BIN" ]; then
  echo "ERROR: uv binary not found after installation"
  exit 1
fi

echo "Found uv at: $UV_BIN"

# Copy to /usr/local/bin so it's available system-wide
cp "$UV_BIN" /usr/local/bin/uv
chmod +x /usr/local/bin/uv

# Verify uv works
if ! /usr/local/bin/uv --version; then
  echo "ERROR: uv installation failed"
  exit 1
fi
echo "uv installed successfully: $(/usr/local/bin/uv --version)"

# Find the EBS volume device with retries (volume attachment happens after instance creation)
# The volume is attached as /dev/sdf, but on newer instances it might be /dev/nvme1n1
EBS_DEVICE=""
echo "Waiting for EBS volume to be attached..."
for i in {1..60}; do
  if [ -b /dev/nvme1n1 ]; then
    EBS_DEVICE="/dev/nvme1n1"
    break
  elif [ -b /dev/sdf ]; then
    EBS_DEVICE="/dev/sdf"
    break
  fi
  if [ $((i % 10)) -eq 0 ]; then
    echo "  Still waiting for EBS volume device... ($i/60)"
  fi
  sleep 2
done

if [ -z "$EBS_DEVICE" ]; then
  echo "ERROR: Could not find EBS volume device after 2 minutes" >&2
  echo "This will cause data to be written to root filesystem instead of EBS volume!" >&2
  exit 1
fi

echo "Found EBS device: $EBS_DEVICE"

# Check if volume needs formatting
if ! blkid $EBS_DEVICE >/dev/null 2>&1; then
  echo "Formatting EBS volume..."
  mkfs.ext4 -F $EBS_DEVICE
  # Wait a moment for filesystem to be ready
  sleep 2
fi

# Ensure mount point exists and is empty
if mountpoint -q /ebs 2>/dev/null; then
  echo "Unmounting existing /ebs mount..."
  umount /ebs || true
fi
mkdir -p /ebs
# Ensure directory is empty (remove any files that might prevent mount)
rm -rf /ebs/* /ebs/.* 2>/dev/null || true

# Mount the volume with retries and verification
echo "Mounting EBS volume $EBS_DEVICE to /ebs..."
MOUNT_SUCCESS=false
for i in {1..30}; do
  # Try to mount with explicit filesystem type
  if mount -t ext4 $EBS_DEVICE /ebs 2>&1; then
    # Wait a moment for mount to settle
    sleep 1
    # Verify mount actually worked using findmnt (more reliable than df)
    MOUNTED_DEVICE=$(findmnt -n -o SOURCE /ebs 2>/dev/null || echo "")
    if [ -n "$MOUNTED_DEVICE" ] && [ "$MOUNTED_DEVICE" != "/" ] && mountpoint -q /ebs; then
      # Double-check: verify the mounted device matches our EBS device
      # Handle both /dev/nvme1n1 and /dev/nvme1n1p1 formats
      if echo "$MOUNTED_DEVICE" | grep -q "$(basename $EBS_DEVICE)"; then
        echo "EBS volume mounted successfully to /ebs (device: $MOUNTED_DEVICE)"
        MOUNT_SUCCESS=true
        break
      else
        echo "Mount device mismatch: expected $EBS_DEVICE, got $MOUNTED_DEVICE, retrying..."
        umount /ebs 2>/dev/null || true
      fi
    else
      echo "Mount verification failed (device: $MOUNTED_DEVICE), retrying..."
      umount /ebs 2>/dev/null || true
    fi
  else
    MOUNT_ERROR=$?
    if [ $((i % 5)) -eq 0 ]; then
      echo "  Mount attempt $i failed (exit code: $MOUNT_ERROR), retrying..."
      # Show what's currently mounted at /ebs if anything
      if mountpoint -q /ebs 2>/dev/null; then
        echo "    Current mount: $(findmnt -n -o SOURCE /ebs 2>/dev/null || echo 'unknown')"
      fi
    fi
  fi
  sleep 2
done

if [ "$MOUNT_SUCCESS" = false ]; then
  echo "ERROR: Failed to mount EBS volume to /ebs after 30 attempts" >&2
  echo "EBS device: $EBS_DEVICE" >&2
  echo "Current /ebs mount: $(findmnt -n -o SOURCE /ebs 2>/dev/null || echo 'not mounted')" >&2
  echo "Root device: $(findmnt -n -o SOURCE / 2>/dev/null || echo 'unknown')" >&2
  echo "This will cause data to be written to root filesystem instead of EBS volume!" >&2
  exit 1
fi

# Final verification we're actually using the EBS volume
MOUNTED_DEVICE=$(findmnt -n -o SOURCE /ebs 2>/dev/null || echo "")
ROOT_DEVICE=$(findmnt -n -o SOURCE / 2>/dev/null || echo "")
if [ "$MOUNTED_DEVICE" = "$ROOT_DEVICE" ] || [ -z "$MOUNTED_DEVICE" ]; then
  echo "ERROR: /ebs mount verification failed!" >&2
  echo "Mounted device: $MOUNTED_DEVICE" >&2
  echo "Root device: $ROOT_DEVICE" >&2
  exit 1
fi

# Check if filesystem needs resizing (if volume was increased)
VOLUME_SIZE=$(blockdev --getsize64 $EBS_DEVICE)
FILESYSTEM_SIZE=$(df -B1 /ebs | tail -1 | awk '{print $2}')
if [ "$VOLUME_SIZE" -gt "$FILESYSTEM_SIZE" ] && [ "$VOLUME_SIZE" -gt 0 ] && [ "$FILESYSTEM_SIZE" -gt 0 ]; then
  echo "Resizing filesystem to match volume size..."
  resize2fs $EBS_DEVICE
fi

# Add to fstab for persistence
EBS_UUID=$(blkid -s UUID -o value $EBS_DEVICE)
if ! grep -q "$EBS_UUID" /etc/fstab; then
  echo "UUID=$EBS_UUID /ebs ext4 defaults,nofail 0 2" >>/etc/fstab
fi

# Create directory structure on EBS volume
mkdir -p /ebs/home
mkdir -p /ebs/shared
mkdir -p /ebs/buildstock # Shared buildstock data directory
mkdir -p /ebs/tmp        # Temporary files directory (for TMPDIR)
chmod 755 /ebs
chmod 755 /ebs/home
chmod 777 /ebs/shared     # Shared directory for all users
chmod 777 /ebs/buildstock # Shared buildstock data for all users
chmod 1777 /ebs/tmp       # Sticky bit for temp directory (all users can write, only owner can delete)

# Set up S3 mount
mkdir -p ${s3_mount_path}
chmod 755 ${s3_mount_path}

# Enable user_allow_other in fuse.conf (required for allow_other mount option)
if ! grep -q "^user_allow_other" /etc/fuse.conf; then
  sed -i 's/#user_allow_other/user_allow_other/' /etc/fuse.conf || echo "user_allow_other" >>/etc/fuse.conf
fi

# Create cache directory for s3fs BEFORE mounting
mkdir -p /tmp/s3fs-cache
chmod 1777 /tmp/s3fs-cache

# Ensure cache directory is recreated on every boot (since /tmp is cleared on reboot)
# This is needed for the fstab mount to work automatically on boot
echo "d /tmp/s3fs-cache 1777 root root -" >/etc/tmpfiles.d/s3fs-cache.conf

# Mount S3 bucket using IAM instance profile
# s3fs automatically uses IAM role when no credentials are specified
# Note: use_path_request_style is required for bucket names with dots (like data.sb)
# Note: endpoint and url are required because bucket is in us-west-2
# umask=0000 allows all users to write to the S3 mount (files appear as 777)
# Without this, files appear as 775 owned by root:root, blocking non-root writes
S3FS_OPTS="_netdev,allow_other,use_cache=/tmp/s3fs-cache,iam_role=auto,umask=0000,use_path_request_style,endpoint=us-west-2,url=https://s3.us-west-2.amazonaws.com"
echo "${s3_bucket_name} ${s3_mount_path} fuse.s3fs $S3FS_OPTS 0 0" >>/etc/fstab

# Try to mount S3 with retries (IAM role may take a moment to be available)
echo "Mounting S3 bucket ${s3_bucket_name} to ${s3_mount_path}..."
for i in 1 2 3 4 5; do
  if mountpoint -q ${s3_mount_path}; then
    echo "S3 already mounted"
    break
  fi

  echo "  Attempt $i: mounting S3..."
  if mount ${s3_mount_path} 2>&1; then
    echo "  S3 mounted successfully"
    break
  else
    echo "  Mount attempt $i failed, waiting before retry..."
    sleep 10
  fi
done

# Final check
if mountpoint -q ${s3_mount_path}; then
  echo "S3 mount verified: $(ls ${s3_mount_path} | head -3)"
else
  echo "WARNING: S3 mount not active after retries. Will be mounted on next boot or login."
fi

# Set TMPDIR system-wide to use EBS volume for temporary files
# This prevents "No space left on device" errors when buildstock-fetch downloads
# large temporary files. Python's tempfile module respects TMPDIR.
if ! grep -q "^TMPDIR=" /etc/environment; then
  echo "TMPDIR=/ebs/tmp" >> /etc/environment
fi

# Start and enable SSM agent (for AWS Systems Manager Session Manager)
systemctl enable amazon-ssm-agent || true
systemctl start amazon-ssm-agent || true

# Install and configure oh-my-zsh for ubuntu user
if id "ubuntu" &>/dev/null; then
  echo "Setting up oh-my-zsh for ubuntu user..."
  cat >/tmp/setup-omz.sh <<'OMZ_SCRIPT'
#!/bin/bash
export HOME="$1"
cd "$HOME"
if [ ! -d "$HOME/.oh-my-zsh" ]; then
    RUNZSH=no sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"
fi
sed -i 's/^ZSH_THEME=.*/ZSH_THEME="bira"/' "$HOME/.zshrc" 2>/dev/null || echo 'ZSH_THEME="bira"' >> "$HOME/.zshrc"
sed -i 's/^plugins=(.*/plugins=(git colored-man-pages colorize history zsh-autosuggestions fast-syntax-highlighting zsh-autocomplete)/' "$HOME/.zshrc" 2>/dev/null || echo 'plugins=(git colored-man-pages colorize history zsh-autosuggestions fast-syntax-highlighting zsh-autocomplete)' >> "$HOME/.zshrc"
mkdir -p "$HOME/.oh-my-zsh/custom/plugins"
[ ! -d "$HOME/.oh-my-zsh/custom/plugins/zsh-autosuggestions" ] && git clone --quiet https://github.com/zsh-users/zsh-autosuggestions.git "$HOME/.oh-my-zsh/custom/plugins/zsh-autosuggestions" || true
[ ! -d "$HOME/.oh-my-zsh/custom/plugins/fast-syntax-highlighting" ] && git clone --quiet https://github.com/zdharma-continuum/fast-syntax-highlighting.git "$HOME/.oh-my-zsh/custom/plugins/fast-syntax-highlighting" || true
[ ! -d "$HOME/.oh-my-zsh/custom/plugins/zsh-autocomplete" ] && git clone --quiet --depth 1 https://github.com/marlonrichert/zsh-autocomplete.git "$HOME/.oh-my-zsh/custom/plugins/zsh-autocomplete" || true
OMZ_SCRIPT
  chmod +x /tmp/setup-omz.sh
  mv /tmp/setup-omz.sh /usr/local/bin/setup-ohmyzsh-for-user.sh
  sudo -u ubuntu /usr/local/bin/setup-ohmyzsh-for-user.sh /home/ubuntu
  if command -v zsh >/dev/null 2>&1; then
    chsh -s "$(command -v zsh)" ubuntu 2>/dev/null || true
  else
    chsh -s /usr/bin/zsh ubuntu 2>/dev/null || true
  fi
fi

# Create a per-boot script to resize root filesystem if needed
cat >/usr/local/bin/resize-root-fs.sh <<'RESIZE_SCRIPT'
#!/bin/bash
ROOT_DEVICE=$(findmnt -n -o SOURCE / | sed 's/p[0-9]*$//' || echo "")
if [ -n "$ROOT_DEVICE" ] && [ -b "$ROOT_DEVICE" ]; then
    ROOT_VOLUME_SIZE=$(blockdev --getsize64 "$ROOT_DEVICE" 2>/dev/null || echo "0")
    ROOT_FS_SIZE=$(df -B1 / | tail -1 | awk '{print $2}')
    if [ "$ROOT_VOLUME_SIZE" -gt "$ROOT_FS_SIZE" ] && [ "$ROOT_VOLUME_SIZE" -gt 0 ]; then
        echo "Resizing root filesystem from $ROOT_FS_SIZE to $ROOT_VOLUME_SIZE..."
        resize2fs "$ROOT_DEVICE" 2>&1
    fi
fi
RESIZE_SCRIPT
chmod +x /usr/local/bin/resize-root-fs.sh

# Create systemd service to run resize on boot
cat >/etc/systemd/system/resize-root-fs.service <<'SERVICE_SCRIPT'
[Unit]
Description=Resize root filesystem if volume was increased
After=local-fs.target
Before=sshd.service

[Service]
Type=oneshot
ExecStart=/usr/local/bin/resize-root-fs.sh
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
SERVICE_SCRIPT
systemctl enable resize-root-fs.service
systemctl start resize-root-fs.service

echo "User-data script completed successfully!"
