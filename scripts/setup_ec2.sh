#!/usr/bin/env bash
# setup_ec2.sh — Run this once on a fresh EC2 instance before starting gojodb.
# Adds swap, configures the OOM killer to protect sshd, and applies kernel
# memory pressure settings that prevent hard lockups.
#
# Usage: sudo bash scripts/setup_ec2.sh

set -euo pipefail

SWAP_SIZE="${SWAP_SIZE:-8G}"   # override with: SWAP_SIZE=4G sudo bash setup_ec2.sh

echo "=== 1. Add ${SWAP_SIZE} swap ==="
if swapon --show | grep -q /swapfile; then
  echo "  Swap already enabled, skipping."
else
  fallocate -l "${SWAP_SIZE}" /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  # Make permanent across reboots.
  if ! grep -q /swapfile /etc/fstab; then
    echo '/swapfile none swap sw 0 0' >> /etc/fstab
  fi
  echo "  Swap enabled: $(swapon --show)"
fi

echo "=== 2. Tune swap aggressiveness (swappiness=10 → prefer RAM) ==="
sysctl -w vm.swappiness=10
if ! grep -q 'vm.swappiness' /etc/sysctl.conf; then
  echo 'vm.swappiness=10' >> /etc/sysctl.conf
fi

echo "=== 3. Protect sshd from the OOM killer ==="
# oom_score_adj of -1000 means the OOM killer will never select this PID.
SSHD_PID=$(pgrep -x sshd | head -1 || true)
if [ -n "${SSHD_PID}" ]; then
  echo -1000 > /proc/"${SSHD_PID}"/oom_score_adj
  echo "  sshd (PID ${SSHD_PID}) protected."
else
  echo "  WARNING: sshd PID not found — manual protection needed."
fi
# Persist via a systemd override so it survives restarts.
if command -v systemctl &>/dev/null && systemctl is-active --quiet ssh 2>/dev/null; then
  mkdir -p /etc/systemd/system/ssh.service.d
  cat > /etc/systemd/system/ssh.service.d/oom.conf <<'EOF'
[Service]
OOMScoreAdjust=-1000
EOF
  systemctl daemon-reload
fi

echo "=== 4. Set vm.min_free_kbytes (keep 256 MiB always free) ==="
# Prevents the kernel from allowing allocations right up to 0 bytes free.
sysctl -w vm.min_free_kbytes=262144
if ! grep -q 'vm.min_free_kbytes' /etc/sysctl.conf; then
  echo 'vm.min_free_kbytes=262144' >> /etc/sysctl.conf
fi

echo "=== 5. Verify Docker is installed ==="
if ! command -v docker &>/dev/null; then
  echo "  Docker not found. Installing..."
  apt-get update -q
  apt-get install -y docker.io docker-compose-plugin
  systemctl enable --now docker
else
  echo "  Docker: $(docker --version)"
fi

echo ""
echo "=== Setup complete ==="
echo "  Free memory : $(free -h | awk '/^Mem/{print $4}')"
echo "  Swap        : $(free -h | awk '/^Swap/{print $2}')"
echo ""
echo "Next steps:"
echo "  cd /path/to/gojodb"
echo "  docker compose build"
echo "  docker compose up -d"
echo "  docker compose logs -f"
