import time
import uuid
import random
import logging
import sys
import os

from datetime import datetime, timezone
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Allow importing config/ from the project root
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from config.settings import RedpandaConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

# ── Event simulation data ────────────────────────────────────────
EVENT_TYPES = ["product_view", "add_to_cart", "checkout", "payment", "search", "login", "review"]
WEIGHTS     = [40, 20, 10, 8, 12, 5, 5]   # realistic e-commerce funnel distribution
PRODUCTS    = [f"prod_{i}" for i in range(1, 200)]
COUNTRIES   = ["IN", "US", "UK", "DE", "SG", "AU"]
DEVICES     = ["mobile", "desktop", "tablet"]


def build_event(session_pool: list) -> dict:
    """Generate one realistic e-commerce event."""
    event_type = random.choices(EVENT_TYPES, weights=WEIGHTS)[0]

    return {
        "event_id":     str(uuid.uuid4()),
        "user_id":      f"user_{random.randint(1, 5000)}",
        "session_id":   random.choice(session_pool),
        "event_type":   event_type,
        "product_id":   random.choice(PRODUCTS) if event_type != "search" else None,
        "search_query": f"query_{random.randint(1, 100)}" if event_type == "search" else None,
        "amount":       round(random.uniform(10, 5000), 2) if event_type == "payment" else None,
        "device_type":  random.choice(DEVICES),
        "geo_country":  random.choice(COUNTRIES),
        "timestamp_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
    }


def delivery_report(err, msg):
    """Called once for each message to confirm delivery success/failure."""
    if err is not None:
        log.error(f"Delivery failed: {err}")
    else:
        log.debug(f"Delivered to {msg.topic()} [partition {msg.partition()}] @ offset {msg.offset()}")


def main():
    cfg = RedpandaConfig.from_env()

    # ── Set up Schema Registry client ────────────────────────────
    schema_registry_client = SchemaRegistryClient({
        "url": cfg.schema_registry_url,
        "basic.auth.user.info": f"{cfg.sasl_username}:{cfg.sasl_password}",
    })

    schema_path = os.path.join(os.path.dirname(__file__), "schemas", "user_activity.avsc")
    with open(schema_path) as f:
        schema_str = f.read()

    avro_serializer = AvroSerializer(schema_registry_client, schema_str)

    # ── Set up Kafka producer ────────────────────────────────────
    producer_conf = cfg.to_producer_config()
    producer_conf.update({
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 5,
    })
    producer = Producer(producer_conf)

    session_pool = [str(uuid.uuid4()) for _ in range(100)]
    rate = float(os.getenv("EVENTS_PER_SECOND", "10"))
    events_sent = 0

    log.info(f"Producer started — target rate: {rate} events/sec")
    log.info("Press Ctrl+C to stop")

    try:
        while True:
            event = build_event(session_pool)

            producer.produce(
                topic="user-activity",
                key=event["user_id"].encode("utf-8"),
                value=avro_serializer(
                    event,
                    SerializationContext("user-activity", MessageField.VALUE)
                ),
                on_delivery=delivery_report,
            )
            producer.poll(0)
            events_sent += 1

            if events_sent % 50 == 0:
                log.info(f"Sent {events_sent} events so far")

            time.sleep(1.0 / rate)

    except KeyboardInterrupt:
        log.info("Stopping producer...")
    finally:
        producer.flush()
        log.info(f"Final count: {events_sent} events sent")


if __name__ == "__main__":
    main()