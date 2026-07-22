CHECKPOINT_ROOT = "/Volumes/workspace/default/ecommerce_data/checkpoints"
S3_ROOT = "s3://ecomm-analytics-delta-2085"

TOPIC_CONFIGS = {

    "user_activity": {
        "kafka_topic": "user-activity",
        "schema_subject": "user-activity-value",
        "bronze_table": "workspace.default.bronze_user_activity",
        "bronze_location": f"{S3_ROOT}/bronze/user_activity/",
        "silver_table": "workspace.default.silver_user_activity",
        "silver_location": f"{S3_ROOT}/silver/user_activity/",
        "dedup_keys": ["event_id"],
        "bronze_checkpoint": f"{CHECKPOINT_ROOT}/bronze_user_activity",
        "silver_checkpoint": f"{CHECKPOINT_ROOT}/silver_user_activity",
        "silver_columns": [
            "event_id", "user_id", "session_id", "event_type", "product_id",
            "search_query", "amount", "device_type", "geo_country",
            "timestamp_ms", "kafka_timestamp", "ingested_at"
        ],
        "silver_ddl": """
            event_id STRING, user_id STRING, session_id STRING, event_type STRING,
            product_id STRING, search_query STRING, amount DOUBLE, device_type STRING,
            geo_country STRING, timestamp_ms LONG, kafka_timestamp TIMESTAMP,
            ingested_at TIMESTAMP, processed_timestamp TIMESTAMP
        """,
        "gold_jobs": [
            {
                "name": "funnel_metrics_hourly",
                "gold_table": "workspace.default.gold_funnel_metrics_hourly",
                "gold_location": f"{S3_ROOT}/gold/funnel_metrics_hourly/",
                "checkpoint": f"{CHECKPOINT_ROOT}/gold_funnel_hourly",
                "group_by": ["event_type"],
                "aggs": {"event_count": "count(*)"}
            },
            {
                "name": "revenue_hourly",
                "gold_table": "workspace.default.gold_revenue_hourly",
                "gold_location": f"{S3_ROOT}/gold/revenue_hourly/",
                "checkpoint": f"{CHECKPOINT_ROOT}/gold_revenue_hourly",
                "filter": "event_type = 'payment'",
                "group_by": [],
                "aggs": {"total_revenue": "sum(amount)", "payment_count": "count(*)"}
            }
        ]
    },

    "product_events": {
        "kafka_topic": "product-events",
        "schema_subject": "product-events-value",
        "bronze_table": "workspace.default.bronze_product_events",
        "bronze_location": f"{S3_ROOT}/bronze/product_events/",
        "silver_table": "workspace.default.silver_product_events",
        "silver_location": f"{S3_ROOT}/silver/product_events/",
        "dedup_keys": ["event_id"],
        "bronze_checkpoint": f"{CHECKPOINT_ROOT}/bronze_product_events",
        "silver_checkpoint": f"{CHECKPOINT_ROOT}/silver_product_events",
        "silver_columns": [
            "event_id", "product_id", "event_type", "product_name", "category",
            "price", "previous_price", "stock_quantity", "previous_stock_quantity",
            "timestamp_ms", "kafka_timestamp", "ingested_at"
        ],
        "silver_ddl": """
            event_id STRING, product_id STRING, event_type STRING, product_name STRING,
            category STRING, price DOUBLE, previous_price DOUBLE, stock_quantity INT,
            previous_stock_quantity INT, timestamp_ms LONG, kafka_timestamp TIMESTAMP,
            ingested_at TIMESTAMP, processed_timestamp TIMESTAMP
        """,
        "gold_jobs": [
            {
                "name": "product_activity_hourly",
                "gold_table": "workspace.default.gold_product_activity_hourly",
                "gold_location": f"{S3_ROOT}/gold/product_activity_hourly/",
                "checkpoint": f"{CHECKPOINT_ROOT}/gold_product_activity_hourly",
                "group_by": ["category", "event_type"],
                "aggs": {"event_count": "count(*)"}
            },
            {
                "name": "price_changes_hourly",
                "gold_table": "workspace.default.gold_price_changes_hourly",
                "gold_location": f"{S3_ROOT}/gold/price_changes_hourly/",
                "checkpoint": f"{CHECKPOINT_ROOT}/gold_price_changes_hourly",
                "filter": "event_type = 'price_changed'",
                "group_by": ["category"],
                "aggs": {
                    "avg_price": "avg(price)",
                    "price_change_count": "count(*)"
                }
            }
        ]
    },

    "order_events": {
        "kafka_topic": "order-events",
        "schema_subject": "order-events-value",
        "bronze_table": "workspace.default.bronze_order_events",
        "bronze_location": f"{S3_ROOT}/bronze/order_events/",
        "silver_table": "workspace.default.silver_order_events",
        "silver_location": f"{S3_ROOT}/silver/order_events/",
        "dedup_keys": ["event_id"],
        "bronze_checkpoint": f"{CHECKPOINT_ROOT}/bronze_order_events",
        "silver_checkpoint": f"{CHECKPOINT_ROOT}/silver_order_events",
        "silver_columns": [
            "event_id", "order_id", "user_id", "event_type", "product_ids",
            "total_amount", "currency", "shipping_country", "cancellation_reason",
            "timestamp_ms", "kafka_timestamp", "ingested_at"
        ],
        "silver_ddl": """
            event_id STRING, order_id STRING, user_id STRING, event_type STRING,
            product_ids ARRAY<STRING>, total_amount DOUBLE, currency STRING,
            shipping_country STRING, cancellation_reason STRING, timestamp_ms LONG,
            kafka_timestamp TIMESTAMP, ingested_at TIMESTAMP, processed_timestamp TIMESTAMP
        """,
        "gold_jobs": [
            {
                "name": "order_metrics_hourly",
                "gold_table": "workspace.default.gold_order_metrics_hourly",
                "gold_location": f"{S3_ROOT}/gold/order_metrics_hourly/",
                "checkpoint": f"{CHECKPOINT_ROOT}/gold_order_metrics_hourly",
                "group_by": ["event_type"],
                "aggs": {"order_count": "count(*)"}
            },
            {
                "name": "order_revenue_hourly",
                "gold_table": "workspace.default.gold_order_revenue_hourly",
                "gold_location": f"{S3_ROOT}/gold/order_revenue_hourly/",
                "checkpoint": f"{CHECKPOINT_ROOT}/gold_order_revenue_hourly",
                "filter": "event_type = 'order_placed'",
                "group_by": ["shipping_country"],
                "aggs": {
                    "total_revenue": "sum(total_amount)",
                    "order_count": "count(*)"
                }
            }
        ]
    }
}