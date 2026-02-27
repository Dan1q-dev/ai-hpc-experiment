FROM ubuntu:24.04

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        munge \
        slurm-wlm \
        slurmctld \
        slurmd \
        tini \
    && usermod -a -G munge slurm \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /etc/slurm /etc/munge /var/lib/munge /run/munge \
    && mkdir -p /var/spool/slurmctld /var/spool/slurmd /var/log/slurm /var/log/munge \
    && dd if=/dev/urandom bs=1 count=1024 of=/etc/munge/munge.key status=none \
    && chown munge:munge /etc/munge/munge.key \
    && chmod 0400 /etc/munge/munge.key \
    && chown -R slurm:slurm /var/spool/slurmctld /var/spool/slurmd /var/log/slurm

COPY slurm.conf /etc/slurm/slurm.conf
COPY cgroup.conf /etc/slurm/cgroup.conf
COPY start-slurm.sh /usr/local/bin/start-slurm.sh

RUN chmod +x /usr/local/bin/start-slurm.sh

EXPOSE 6817 6818

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/local/bin/start-slurm.sh"]
