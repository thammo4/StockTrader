# Dockerfile

# Apple Silicon (ARM64) Compatible Base Image
# FROM --platform=linux/arm64 python3.12=slim
# FROM python:3.12-slim
# FROM --platform=linux/arm64 python:3.12-slim
FROM python:3.12-slim

# Define In-Container Working Directory
# WORKDIR /app

# Copy Project Files From Host -> Container (config/metadata, source, aux scripts)
# COPY <src> <target>

COPY pyproject.toml ./
COPY src/ ./src/
COPY scripts/ ./scripts/

# Install Dependencies
# RUN pip install --upgrade pip && pip install .
RUN pip install --upgrade --force-reinstall --no-cache-dir six
RUN pip install --upgrade --force-reinstall --no-cache-dir pandas
RUN pip install --upgrade pip && pip install .

# Initialize interactive python session for testing
CMD ["python3"]
