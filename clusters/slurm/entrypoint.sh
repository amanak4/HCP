#!/bin/bash
set -euo pipefail

ROLE="${SLURM_ROLE:-compute}"
SHARED_MUNGE="/shared/munge"
mkdir -p "$SHARED_MUNGE" /etc/munge /run/munge /var/run/munge /var/spool/slurmctld /var/spool/slurmd /var/log/slurm /data/jobs
chown munge:munge /run/munge /var/run/munge
chmod 755 /run/munge /var/run/munge

if [[ "$ROLE" == "controller" ]]; then
  if [[ ! -s "$SHARED_MUNGE/munge.key" ]]; then
    dd if=/dev/urandom bs=1 count=1024 of="$SHARED_MUNGE/munge.key" status=none
    chmod 400 "$SHARED_MUNGE/munge.key"
  fi
else
  for _ in $(seq 1 60); do
    if [[ -s "$SHARED_MUNGE/munge.key" ]]; then
      break
    fi
    sleep 1
  done
  if [[ ! -s "$SHARED_MUNGE/munge.key" ]]; then
    echo "timed out waiting for munge.key from controller" >&2
    exit 1
  fi
fi

cp "$SHARED_MUNGE/munge.key" /etc/munge/munge.key
chown munge:munge /etc/munge /etc/munge/munge.key
chmod 700 /etc/munge
chmod 400 /etc/munge/munge.key

chown slurm:slurm /var/spool/slurmctld /var/log/slurm
chmod 755 /var/spool/slurmctld /var/spool/slurmd /var/log/slurm

if pgrep munged >/dev/null 2>&1; then
  pkill munged || true
  sleep 0.5
fi
runuser -u munge -- /usr/sbin/munged --force

echo "starting slurm role=$ROLE hostname=$(hostname)"

if [[ "$ROLE" == "controller" ]]; then
  exec /usr/sbin/slurmctld -Dvvv
fi

exec /usr/sbin/slurmd -Dvvv
