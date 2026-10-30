# ==============================================================================
# Production-Style Data Lakehouse Platform - Multi-Stage Dockerfile
# ==============================================================================
FROM python:3.11-slim AS base

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH="/usr/lib/jvm/java-17-openjdk-amd64/bin:${PATH}"

# Install OpenJDK and system utilities
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    build-essential \
    curl \
    git \
    procps \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency specifications
COPY requirements.txt pyproject.toml ./

# Install Python packages
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy source code and configuration
COPY src/ ./src/
COPY dags/ ./dags/
COPY dbt_project/ ./dbt_project/
COPY schemas/ ./schemas/
COPY sql/ ./sql/
COPY spark/ ./spark/
COPY scripts/ ./scripts/
COPY dashboard/ ./dashboard/
COPY tests/ ./tests/
COPY .env.example ./
RUN cp .env.example .env

# Create data directories with appropriate permissions
RUN mkdir -p data/lakehouse/bronze data/lakehouse/silver data/lakehouse/gold data/lakehouse/quarantine logs

EXPOSE 8501 8080

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import duckdb, pyarrow, pandas, sqlalchemy; print('OK')" || exit 1

CMD ["python", "scripts/run_pipeline.py", "--batch-type", "INCREMENTAL"]
