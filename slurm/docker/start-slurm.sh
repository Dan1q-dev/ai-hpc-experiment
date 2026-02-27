#!/usr/bin/env bash
set -euo pipefail

export SLURM_CONF=/etc/slurm/slurm.conf

mkdir -p /run/munge /var/log/munge /var/log/slurm /var/spool/slurmctld /var/spool/slurmd
mkdir -p /sys/fs/cgroup/system.slice || true

chown -R munge:munge /run/munge /var/log/munge /etc/munge /var/lib/munge
chmod 0700 /etc/munge /var/lib/munge
chmod 0755 /run/munge
chmod 0400 /etc/munge/munge.key
chown munge:munge /etc/munge/munge.key

chown -R slurm:slurm /var/log/slurm /var/spool/slurmctld /var/spool/slurmd

munged --force --syslog
slurmctld -f "${SLURM_CONF}"

# Keep container alive on slurmd foreground process.
exec slurmd -f "${SLURM_CONF}" -D -N slurm-local -vv
