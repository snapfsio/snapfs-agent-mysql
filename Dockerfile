FROM python:3.12-slim

# Fast startup, clean logs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (curl just for debugging/healthchecks if you want)
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml README.md LICENSE /app/
COPY src /app/src

# Install the agent package
RUN pip install --no-cache-dir .

# Runtime configuration (override in compose/k8s)
ENV GATEWAY_WS="ws://gateway:8000" \
    MYSQL_URL="mysql+aiomysql://snapfs:snapfs@mysql:3306/snapfs" \
    SNAPFS_SUBJECT="snapfs.files" \
    SNAPFS_DURABLE="mysql" \
    SNAPFS_BATCH="100"

# Agent is outbound-only; no ports exposed
CMD ["python", "-m", "snapfs_agent_mysql.main"]
