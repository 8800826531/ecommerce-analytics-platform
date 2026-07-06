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

# ── Order lifecycle simulation ───────────────────────────────────
EVENT_TYPES = ["order_placed", "order_confirmed", "order_shipped", "order_delivered", "order_cancelled"]
WEIGHTS     = [35, 25, 20, 15, 5]   # most orders are newly placed, few get cancelled

PRODUCTS   = [f"prod_{i}" for i in range(1, 200)]
COUNTRIES  = ["IN", "US", "UK", "DE", "SG", "AU"]
CURRENCIES = {"IN": "INR", "US": "USD", "UK": "GBP", "DE": "EUR", "SG": "SGD", "AU": "AUD"}

CANCELLATION_REASONS = [
    "customer_requested", "payment_failed", "out_of_stock",
    "fraud_detected", "address_invalid"
]

# Tracks orders currently "in flight" so lifecycle events
# (confirmed, shipped, delivered) reference a real order_id
active_orders = {}


def build_event() -> dict:
    event_type = random.choices(EVENT_TYPES, weights=WEIGHTS)[0]

    if event_type == "order_placed" or not active_orders:
        # Create a brand new order
        order_id = f"ord_{uuid.uuid4().hex[:12]}"
        country = random.choice(COUNTRIES)
        num_products = random.randint(1, 4)
        order = {
            "order_id": order_id,
            "user_id": f"user_{random.randint(1, 5000)}",
            "product_ids": random.sample(PRODUCTS, num_products),
            "total_amount": round(random.uniform(20, 3000), 2),
            "currency": CURRENCIES[country],
            "shipping_country": country,
        }
        active_orders[order_id] = order
        event_type = "order_placed"
    else:
        # Progress an existing order through its lifecycle
        order_id = random.choice(list(active_orders.keys()))
        order = active_orders[order_id]

    cancellation_reason = None
    if event_type == "order_cancelled":
        cancellation_reason = random.choice(CANCELLATION_REASONS)
        active_orders.pop(order_id, None)  # remove from active tracking

    elif event_type == "order_delivered":
        active_orders.pop(order_id, None)  # order lifecycle complete

    return {
        "event_id":            str(uuid.uuid4()),
        "order_id":             order["order_id"],
        "user_id":              order["user_id"],
        "event_type":           event_type,
        "product_ids":          order["product_ids"],
        "total_amount":         order["total_amount"],
        "currency":             order["currency"],
        "shipping_country":     order["shipping_country"],
        "cancellation_reason":  cancellation_reason,
        "timestamp_ms":         int(datetime.now(timezone.utc).timestamp() * 1000),
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

    schema_path = os.path.join(os.path.dirname(__file__), "schemas", "order_event.avsc")
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

    rate = float(os.getenv("EVENTS_PER_SECOND", "3"))
    events_sent = 0

    log.info(f"Order event producer started — target rate: {rate} events/sec")
    log.info("Press Ctrl+C to stop")

    try:
        while True:
            event = build_event()

            producer.produce(
                topic="order-events",
                key=event["order_id"].encode("utf-8"),
                value=avro_serializer(
                    event,
                    SerializationContext("order-events", MessageField.VALUE)
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