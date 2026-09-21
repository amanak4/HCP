FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        slurm-wlm \
        slurmctld \
        slurmd \
        slurm-client \
        slurm-wlm-basic-plugins \
        munge \
        ca-certificates \
        bash \
        util-linux \
        procps \
    && rm -rf /var/lib/apt/lists/*

COPY slurm.conf /etc/slurm/slurm.conf
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN sed -i 's/\r$//' /usr/local/bin/entrypoint.sh /etc/slurm/slurm.conf \
    && chmod +x /usr/local/bin/entrypoint.sh \
    && mkdir -p /var/spool/slurmctld /var/spool/slurmd /var/log/slurm /data/jobs \
    && chown slurm:slurm /var/spool/slurmctld /var/log/slurm

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
