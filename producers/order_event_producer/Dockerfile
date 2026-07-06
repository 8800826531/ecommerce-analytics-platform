FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements/ ./requirements/
COPY producers/order_event_producer/requirements.txt ./producer-requirements.txt
RUN pip install --no-cache-dir -r requirements/producers.txt

COPY config/ ./config/
COPY producers/order_event_producer/ .

ENV EVENTS_PER_SECOND=3

CMD ["python", "main.py"]