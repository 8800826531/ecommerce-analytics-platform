from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable


def ensure_silver_table(spark, config: dict):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config['silver_table']} (
            {config['silver_ddl']}
        )
        USING DELTA
    """)


def clean_silver(spark, config: dict):
    ensure_silver_table(spark, config)

    bronze_stream = spark.readStream.format("delta").table(config["bronze_table"])

    def upsert_batch(microBatchDF, batch_id):
        deduped = microBatchDF.dropDuplicates(config["dedup_keys"])
        deduped = deduped.select(*config["silver_columns"]) \
                          .withColumn("processed_timestamp", current_timestamp())

        target = DeltaTable.forName(spark, config["silver_table"])
        merge_condition = " AND ".join(
            [f"target.{k} = source.{k}" for k in config["dedup_keys"]]
        )
        (
            target.alias("target")
            .merge(deduped.alias("source"), merge_condition)
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"[{config['silver_table']}] Batch {batch_id}: {deduped.count()} rows")

    query = (
        bronze_stream.writeStream
        .foreachBatch(upsert_batch)
        .option("checkpointLocation", config["silver_checkpoint"])
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    row_count = spark.table(config["silver_table"]).count()
    print(f"[{config['silver_table']}] Silver total rows: {row_count}")
    return row_count