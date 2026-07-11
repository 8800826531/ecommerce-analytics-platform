from pyspark.sql.functions import col, window, expr, to_timestamp


def run_gold_aggregation(spark, silver_table: str, job_spec: dict,
                          watermark="30 minutes", window_dur="1 hour"):
    stream = (
        spark.readStream
        .format("delta")
        .table(silver_table)
        .withColumn("event_time", to_timestamp(col("timestamp_ms") / 1000))
        .withWatermark("event_time", watermark)
    )

    if "filter" in job_spec:
        stream = stream.filter(job_spec["filter"])

    group_cols = [window(col("event_time"), window_dur)] + [col(c) for c in job_spec["group_by"]]
    agg_exprs = [expr(f"{expr_str} as {alias}") for alias, expr_str in job_spec["aggs"].items()]

    result = stream.groupBy(*group_cols).agg(*agg_exprs)

    select_cols = [col("window.start").alias("window_start"), col("window.end").alias("window_end")]
    select_cols += [col(c) for c in job_spec["group_by"]]
    select_cols += [col(alias) for alias in job_spec["aggs"].keys()]
    result = result.select(*select_cols)

    query = (
        result.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", job_spec["checkpoint"])
        .trigger(availableNow=True)
        .toTable(job_spec["gold_table"])
    )
    query.awaitTermination()

    row_count = spark.table(job_spec["gold_table"]).count()
    print(f"[{job_spec['gold_table']}] rows: {row_count}")
    return row_count


def run_all_gold_jobs(spark, config: dict):
    results = {}
    for job_spec in config["gold_jobs"]:
        results[job_spec["name"]] = run_gold_aggregation(spark, config["silver_table"], job_spec)
    return results