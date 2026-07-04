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

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from config.settings import RedpandaConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

# ── Product catalog simulation ───────────────────────────────────
EVENT_TYPES = ["product_created", "product_updated", "price_changed", "stock_updated"]
WEIGHTS     = [5, 15, 30, 50]   # most events are stock updates, few are new products

CATEGORIES = ["electronics", "clothing", "home", "sports", "books", "toys"]
PRODUCT_NAMES = [
    "Wireless Headphones", "Running Shoes", "Coffee Maker", "Yoga Mat",
    "Bluetooth Speaker", "Desk Lamp", "Backpack", "Water Bottle",
    "Laptop Stand", "Phone Case"
]

# In-memory product state — tracks current price/stock so we can
# generate realistic "previous" values for change events
product_state = {}


def get_or_create_product(product_id: str) -> dict:
    if product_id not in product_state:
        product_state[product_id] = {
            "name": random.choice(PRODUCT_NAMES),
            "category": random.choice(CATEGORIES),
            "price": round(random.uniform(10, 500), 2),
            "stock": random.randint(0, 500),
        }
    return product_state[product_id]


def build_event(product_pool: list) -> dict:
    product_id = random.choice(product_pool)
    product = get_or_create_product(product_id)
    event_type = random.choices(EVENT_TYPES, weights=WEIGHTS)[0]

    previous_price = None
    previous_stock = None

    if event_type == "price_changed":
        previous_price = product["price"]
        product["price"] = round(product["price"] * random.uniform(0.85, 1.15), 2)

    elif event_type == "stock_updated":
        previous_stock = product["stock"]
        product["stock"] = max(0, product["stock"] + random.randint(-20, 50))

    return {
        "event_id":                 str(uuid.uuid4()),
        "product_id":               product_id,
        "event_type":               event_type,
        "product_name":             product["name"],
        "category":                 product["category"],
        "price":                    product["price"],
        "previous_price":           previous_price,
        "stock_quantity":           product["stock"],
        "previous_stock_quantity":  previous_stock,
        "timestamp_ms":             int(datetime.now(timezone.utc).timestamp() * 1000),
    }


def delivery_report(err, msg):
    if err is not None:
        log.error(f"Delivery failed: {err}")
    else:
        log.debug(f"Delivered to {msg.topic()} [partition {msg.partition()}] @ offset {msg.offset()}")


def main():
    cfg = RedpandaConfig.from_env()

    schema_registry_client = SchemaRegistryClient({
        "url": cfg.schema_registry_url,
        "basic.auth.user.info": f"{cfg.sasl_username}:{cfg.sasl_password}",
    })

    schema_path = os.path.join(os.path.dirname(__file__), "schemas", "product_event.avsc")
    with open(schema_path) as f:
        schema_str = f.read()

    avro_serializer = AvroSerializer(schema_registry_client, schema_str)

    producer_conf = cfg.to_producer_config()
    producer_conf.update({
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 5,
    })
    producer = Producer(producer_conf)

    # Simulate a catalog of 200 products
    product_pool = [f"prod_{i}" for i in range(1, 200)]
    rate = float(os.getenv("EVENTS_PER_SECOND", "5"))
    events_sent = 0

    log.info(f"Product event producer started — target rate: {rate} events/sec")
    log.info("Press Ctrl+C to stop")

    try:
        while True:
            event = build_event(product_pool)

            producer.produce(
                topic="product-events",
                key=event["product_id"].encode("utf-8"),
                value=avro_serializer(
                    event,
                    SerializationContext("product-events", MessageField.VALUE)
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