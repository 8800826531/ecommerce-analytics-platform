import requests
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr, current_timestamp


def get_schema(schema_registry_url, subject, username, password):
    resp = requests.get(
        f"{schema_registry_url}/subjects/{subject}/versions/latest",
        auth=(username, password)
    )
    resp.raise_for_status()
    return resp.json()["schema"]


def build_jaas_config(username, password):
    return (
        f'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required '
        f'username="{username}" password="{password}";'
    )


def read_topic_stream(spark, bootstrap_servers, jaas_config, topic):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def decode_avro(df, schema_str):
    return (
        df.withColumn("avro_payload", expr("substring(value, 6, length(value) - 5)"))
        .withColumn("decoded", from_avro(col("avro_payload"), schema_str))
        .select(
            "decoded.*",
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp")
        )
        .withColumn("ingested_at", current_timestamp())
    )


def ingest_bronze(spark, config: dict, creds: dict):
    """
    config: one entry from TOPIC_CONFIGS (e.g. TOPIC_CONFIGS["user_activity"])
    creds: {
        "bootstrap_servers": ...,
        "username": ...,
        "password": ...,
        "schema_registry_url": ...,
    }
    """
    jaas_config = build_jaas_config(creds["username"], creds["password"])

    kafka_stream = read_topic_stream(
        spark, creds["bootstrap_servers"], jaas_config, config["kafka_topic"]
    )

    schema_str = get_schema(
        creds["schema_registry_url"],
        config["schema_subject"],
        creds["username"],
        creds["password"]
    )

    parsed = decode_avro(kafka_stream, schema_str)

    query = (
        parsed.writeStream
        .format("delta")
        .option("checkpointLocation", config["bronze_checkpoint"])
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(config["bronze_table"])
    )
    query.awaitTermination()

    row_count = spark.table(config["bronze_table"]).count()
    print(f"[{config['kafka_topic']}] Bronze rows written. Table total: {row_count}")
    return row_count