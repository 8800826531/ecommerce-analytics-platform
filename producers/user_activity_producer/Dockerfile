# Use official Python slim image — smaller size, faster builds
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Install system dependency needed by confluent-kafka
RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the shared requirements folder first (Docker layer caching)
COPY requirements/ ./requirements/

# Copy this producer's own requirements.txt
COPY producers/user_activity_producer/requirements.txt ./producer-requirements.txt

# Install using the producer's requirements file
# It references ../../requirements/producers.txt via -r,
# so we install directly from the producers.txt path instead
RUN pip install --no-cache-dir -r requirements/producers.txt

# Copy the config module from project root
COPY config/ ./config/

# Copy the producer code
COPY producers/user_activity_producer/ .

# Environment variable with default — overridable at runtime
ENV EVENTS_PER_SECOND=10

# Run the producer
CMD ["python", "main.py"]