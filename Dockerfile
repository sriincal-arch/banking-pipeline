FROM python:3.11-slim

# Set working directory
WORKDIR /opt/dagster/app

# Install system dependencies including MinIO, supervisord, and bash utilities
RUN apt-get update && apt-get install -y \
    git \
    curl \
    wget \
    supervisor \
    bash-completion \
    vim \
    less \
    && rm -rf /var/lib/apt/lists/*

# Configure bash environment with proper prompt and shortcuts
RUN echo 'export PS1="\[\033[01;32m\]\u@banking-pipeline\[\033[00m\]:\[\033[01;34m\]\w\[\033[00m\]\$ "' >> /etc/bash.bashrc && \
    echo 'alias ll="ls -la"' >> /etc/bash.bashrc && \
    echo 'alias la="ls -A"' >> /etc/bash.bashrc && \
    echo 'alias l="ls -CF"' >> /etc/bash.bashrc && \
    echo 'alias ..="cd .."' >> /etc/bash.bashrc && \
    echo 'alias ...="cd ../.."' >> /etc/bash.bashrc && \
    echo 'alias dbt-run="cd /opt/dagster/app/dbt && dbt run"' >> /etc/bash.bashrc && \
    echo 'alias dbt-test="cd /opt/dagster/app/dbt && dbt test"' >> /etc/bash.bashrc && \
    echo 'alias pipeline="run-pipeline"' >> /etc/bash.bashrc && \
    echo 'alias logs="tail -f /var/log/supervisor/*.log"' >> /etc/bash.bashrc && \
    echo 'if [ -f /etc/bash_completion ]; then . /etc/bash_completion; fi' >> /etc/bash.bashrc && \
    echo 'cd /opt/dagster/app' >> /etc/bash.bashrc

# Install MinIO server
RUN wget https://dl.min.io/server/minio/release/linux-amd64/minio -O /usr/local/bin/minio \
    && chmod +x /usr/local/bin/minio

# Install MinIO client (mc)
RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc -O /usr/local/bin/mc \
    && chmod +x /usr/local/bin/mc

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    dagster \
    dagster-webserver \
    dagster-duckdb \
    dagster-dbt \
    dbt-core \
    dbt-duckdb \
    duckdb \
    boto3 \
    pandas \
    pyarrow

# Verify dbt installation
RUN dbt --version

# Create necessary directories
RUN mkdir -p /opt/dagster/dagster_home \
    /opt/dagster/data \
    /opt/dagster/data/output \
    /opt/dagster/data/sample \
    /data/minio \
    /var/log/supervisor

# Copy dagster.yaml configuration
COPY banking_pipeline/dagster.yaml /opt/dagster/dagster_home/dagster.yaml

# Copy application code
COPY banking_pipeline /opt/dagster/app/banking_pipeline
COPY dbt /opt/dagster/app/dbt

# Copy sample data files (these will be uploaded to MinIO on startup)
COPY data/sample /opt/dagster/data/sample

# Copy scripts
COPY scripts/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY scripts/run-pipeline.sh /usr/local/bin/run-pipeline
COPY scripts/supervisord.conf /etc/supervisor/conf.d/banking-pipeline.conf

# Make scripts executable
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/run-pipeline

# Install dbt packages
WORKDIR /opt/dagster/app/dbt
RUN dbt deps || true
WORKDIR /opt/dagster/app

# Set environment variables
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONPATH=/opt/dagster/app
ENV DBT_PROFILES_DIR=/opt/dagster/app/dbt
ENV DUCKDB_DATABASE=/opt/dagster/data/banking.duckdb
ENV MINIO_ENDPOINT=http://localhost:9000
ENV MINIO_ACCESS_KEY=minioadmin
ENV MINIO_SECRET_KEY=minioadmin
ENV TERM=xterm-256color

# Expose ports: 3000 (Dagster), 9000 (MinIO API), 9001 (MinIO Console)
EXPOSE 3000 9000 9001

# Use entrypoint script that auto-runs the pipeline
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
