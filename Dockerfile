FROM python:3.11-slim

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Java for Spark support (optional)
RUN apt-get update && apt-get install -y openjdk-11-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Copy requirements and install Python dependencies
COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install -e ".[all]"

# Copy source code
COPY src/ ./src/
COPY config/ ./config/
COPY scripts/ ./scripts/

# Create necessary directories
RUN mkdir -p data/{raw,processed,external} logs reports

# Expose port for any web interfaces
EXPOSE 8080

# Default command
CMD ["data-pipeline", "--help"]