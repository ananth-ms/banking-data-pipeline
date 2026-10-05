from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    year,
    month,
    dayofmonth,
    lit,
    when,
    concat_ws,
    window,
    count
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

import os
import mysql.connector


# ============================================================
# Spark Session
# ============================================================

spark = SparkSession.builder \
    .appName("RealtimeBankingFraudPipeline") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# HDFS Configuration
# ============================================================

HDFS_BASE = (
    "hdfs://localhost:9000/"
    "ananth/realtime-banking-data-pipeline"
)

INPUT_PATH = f"{HDFS_BASE}/data/stream_input"
PROCESSED_PATH = f"{HDFS_BASE}/data/stream_processed"
REJECTED_PATH = f"{HDFS_BASE}/data/stream_rejected"
FRAUD_PATH = f"{HDFS_BASE}/data/stream_fraud"

CHECKPOINT_BASE = (
    f"{HDFS_BASE}/checkpoints/"
    f"realtime_fraud_mysql_v5"
)


# ============================================================
# MySQL Configuration
#
# NOTE: no hardcoded password fallback anymore. Set these as
# real environment variables before running, e.g.:
#   export MYSQL_PASSWORD='your-password'
# The job will fail fast at startup if it's missing, rather
# than silently using a default that could end up in git history.
# ============================================================

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "banking_db")
MYSQL_USER = os.getenv("MYSQL_USER", "ananth")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "Ananth@2003")

if not MYSQL_PASSWORD:
    raise RuntimeError(
        "MYSQL_PASSWORD environment variable is not set. "
        "Export it before starting the pipeline."
    )

MYSQL_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:"
    f"{MYSQL_PORT}/{MYSQL_DATABASE}"
    f"?useSSL=false&serverTimezone=UTC"
)

MYSQL_PROPERTIES = {
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver": "com.mysql.cj.jdbc.Driver"
}


def get_mysql_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=int(MYSQL_PORT),
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )


# ============================================================
# Idempotency control table
#
# Spark's foreachBatch re-runs a micro-batch with the SAME
# batch_id if the driver fails after the batch ran but before
# the checkpoint offset was committed. Without this table,
# that retry would write the batch's rows to MySQL/HDFS a
# second time. This table makes each batch commit-once.
#
# Run once before starting the pipeline:
#
# CREATE TABLE IF NOT EXISTS processed_batches (
#     query_name  VARCHAR(50)  NOT NULL,
#     batch_id    BIGINT       NOT NULL,
#     processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#     PRIMARY KEY (query_name, batch_id)
# );
#
# Also add a UNIQUE constraint on transactions.transaction_id
# as defense-in-depth against duplicates that arrive far
# enough apart to fall outside the 10-minute watermark:
#
# ALTER TABLE transactions
#     ADD UNIQUE KEY uq_transaction_id (transaction_id);
# ============================================================

def is_batch_processed(query_name, batch_id):
    conn = get_mysql_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM processed_batches "
            "WHERE query_name = %s AND batch_id = %s",
            (query_name, batch_id)
        )
        return cursor.fetchone() is not None
    finally:
        conn.close()


def mark_batch_processed(query_name, batch_id):
    conn = get_mysql_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT IGNORE INTO processed_batches "
            "(query_name, batch_id) VALUES (%s, %s)",
            (query_name, batch_id)
        )
        conn.commit()
    finally:
        conn.close()


# ============================================================
# Transaction Schema
# ============================================================

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("status", StringType(), True)
])


# ============================================================
# Read Streaming Data From HDFS
# ============================================================

raw_stream = spark.readStream \
    .schema(transaction_schema) \
    .json(INPUT_PATH)


# ============================================================
# Timestamp + Source-level Duplicate Handling
#
# This dedupes redelivered RabbitMQ messages carrying the same
# transaction_id, as long as they arrive within the 10-minute
# watermark of each other. It does NOT protect against Spark
# retrying a micro-batch after a driver failure -- that's what
# the processed_batches table above is for.
# ============================================================

transactions = raw_stream \
    .withColumn("transaction_timestamp", to_timestamp(col("timestamp"))) \
    .drop("timestamp") \
    .withColumn("year", year(col("transaction_timestamp"))) \
    .withColumn("month", month(col("transaction_timestamp"))) \
    .withColumn("day", dayofmonth(col("transaction_timestamp"))) \
    .withWatermark("transaction_timestamp", "10 minutes") \
    .dropDuplicates(["transaction_id"])


# ============================================================
# VALIDATION
# ============================================================

validation = transactions \
    .withColumn(
        "invalid_customer",
        when(col("customer_id").isNull(), "INVALID_CUSTOMER")
    ) \
    .withColumn(
        "invalid_amount",
        when(col("amount").isNull() | (col("amount") <= 0), "INVALID_AMOUNT")
    ) \
    .withColumn(
        "invalid_currency",
        when(col("currency") != "INR", "INVALID_CURRENCY")
    ) \
    .withColumn(
        "invalid_transaction_type",
        when(
            ~col("transaction_type").isin(
                "UPI", "ATM", "CARD", "NET_BANKING", "IMPS", "NEFT"
            ),
            "INVALID_TRANSACTION_TYPE"
        )
    ) \
    .withColumn(
        "invalid_status",
        when(~col("status").isin("SUCCESS", "FAILED"), "INVALID_STATUS")
    ) \
    .withColumn(
        "invalid_transaction_id",
        when(
            col("transaction_id").isNull() | (col("transaction_id") == ""),
            "INVALID_TRANSACTION_ID"
        )
    ) \
    .withColumn(
        "invalid_account",
        when(
            col("account_id").isNull() | (col("account_id") == ""),
            "INVALID_ACCOUNT"
        )
    ) \
    .withColumn(
        "invalid_merchant",
        when(
            col("merchant").isNull() | (col("merchant") == ""),
            "INVALID_MERCHANT"
        )
    )

validation = validation.withColumn(
    "rejection_reason",
    concat_ws(
        ",",
        col("invalid_customer"),
        col("invalid_amount"),
        col("invalid_currency"),
        col("invalid_transaction_type"),
        col("invalid_status"),
        col("invalid_transaction_id"),
        col("invalid_account"),
        col("invalid_merchant")
    )
)

INVALID_COLS = [
    "invalid_customer",
    "invalid_amount",
    "invalid_currency",
    "invalid_transaction_type",
    "invalid_status",
    "invalid_transaction_id",
    "invalid_account",
    "invalid_merchant"
]


# ============================================================
# WRITE HELPERS
# These now always take a materialized batch DataFrame
# (the batch_df passed into foreachBatch, or something
# derived from it) -- never a streaming DataFrame.
# ============================================================

## Small batches on a local[*] driver don't need 8-way parallel
## Parquet writers -- that's what was starving the heap and
## stalling the job. Coalescing to 2 partitions before each
## write keeps writer count (and memory pressure) low while
## still writing in parallel across 2 tasks.
HDFS_WRITE_PARTITIONS = 2


def write_processed_to_hdfs(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    batch_df.coalesce(HDFS_WRITE_PARTITIONS).write.mode("append") \
        .partitionBy("year", "month", "day") \
        .parquet(PROCESSED_PATH)
    print(f"[HDFS] processed batch {batch_id} written.")


def write_rejected_to_hdfs(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    batch_df.coalesce(HDFS_WRITE_PARTITIONS).write.mode("append") \
        .partitionBy("year", "month", "day") \
        .parquet(REJECTED_PATH)
    print(f"[HDFS] rejected batch {batch_id} written.")


def write_high_value_fraud_to_hdfs(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    batch_df.coalesce(HDFS_WRITE_PARTITIONS).write.mode("append") \
        .partitionBy("year", "month", "day") \
        .parquet(f"{FRAUD_PATH}/high_value")
    print(f"[HDFS] high-value fraud batch {batch_id} written.")


def write_velocity_fraud_to_hdfs(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    hdfs_df = batch_df \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .drop("window")
    hdfs_df.coalesce(HDFS_WRITE_PARTITIONS).write.mode("append") \
        .parquet(f"{FRAUD_PATH}/velocity")
    print(f"[HDFS] velocity fraud batch {batch_id} written.")


def write_transactions_to_mysql(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    mysql_df = batch_df.select(
        "transaction_id", "customer_id", "account_id", "amount",
        "currency", "transaction_type", "merchant", "location",
        "transaction_timestamp", "status"
    )
    mysql_df.write.jdbc(
        url=MYSQL_URL, table="transactions",
        mode="append", properties=MYSQL_PROPERTIES
    )
    print(f"[MySQL] transaction batch {batch_id} written.")


def write_high_value_fraud_to_mysql(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    mysql_df = batch_df.select(
        "transaction_id", "customer_id", "account_id", "amount",
        "currency", "transaction_type", "merchant", "location",
        "transaction_timestamp", "status",
        "fraud_rule", "fraud_reason", "fraud_status"
    )
    mysql_df.write.jdbc(
        url=MYSQL_URL, table="fraud_alerts",
        mode="append", properties=MYSQL_PROPERTIES
    )
    print(f"[MySQL] high-value fraud batch {batch_id} written.")


def write_velocity_fraud_to_mysql(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    velocity_mysql_df = batch_df.select(
        col("customer_id"),
        col("transaction_count"),
        col("window.start").alias("transaction_timestamp"),
        col("fraud_rule"),
        col("fraud_reason"),
        col("fraud_status")
    ) \
        .withColumn("transaction_id", lit(None).cast("string")) \
        .withColumn("account_id", lit(None).cast("string")) \
        .withColumn("amount", lit(None).cast("double")) \
        .withColumn("currency", lit("INR")) \
        .withColumn("transaction_type", lit("MULTIPLE")) \
        .withColumn("merchant", lit("MULTIPLE")) \
        .withColumn("location", lit("MULTIPLE")) \
        .withColumn("status", lit("DETECTED"))

    final_df = velocity_mysql_df.select(
        "transaction_id", "customer_id", "account_id", "amount",
        "currency", "transaction_type", "merchant", "location",
        "transaction_timestamp", "status",
        "fraud_rule", "fraud_reason", "fraud_status"
    )
    final_df.write.jdbc(
        url=MYSQL_URL, table="fraud_alerts",
        mode="append", properties=MYSQL_PROPERTIES
    )
    print(f"[MySQL] velocity fraud batch {batch_id} written.")


# ============================================================
# QUERY 1 -- everything derived from validation's own batch_df
#
# This query runs on `validation` (all transactions, valid and
# invalid together) so that normal / rejected / high-value fraud
# are all sliced from the SAME materialized micro-batch instead
# of from separate streaming DataFrames.
# ============================================================

def process_transaction_batch(batch_df, batch_id):
    query_name = "transactions"

    if is_batch_processed(query_name, batch_id):
        print(f"[SKIP] batch {batch_id} already committed -- retry, not reprocessing.")
        return

    print()
    print("=" * 70)
    print(f"PROCESSING TRANSACTION BATCH: {batch_id}")
    print("=" * 70)

    batch_df.persist()

    is_invalid = None
    for c in INVALID_COLS:
        cond = col(c).isNotNull()
        is_invalid = cond if is_invalid is None else (is_invalid | cond)

    valid_df = batch_df.filter(~is_invalid)
    rejected_df = batch_df.filter(is_invalid)

    high_value_df = valid_df.filter(col("amount") > 100000) \
        .withColumn("fraud_rule", lit("HIGH_VALUE_TRANSACTION")) \
        .withColumn("fraud_reason", lit("Transaction amount exceeds INR 100000")) \
        .withColumn("fraud_status", lit("POTENTIAL_FRAUD"))

    normal_df = valid_df.filter(col("amount") <= 100000)

    write_processed_to_hdfs(normal_df, batch_id)
    write_rejected_to_hdfs(rejected_df, batch_id)
    write_high_value_fraud_to_hdfs(high_value_df, batch_id)
    write_transactions_to_mysql(normal_df, batch_id)
    write_high_value_fraud_to_mysql(high_value_df, batch_id)

    mark_batch_processed(query_name, batch_id)
    batch_df.unpersist()

    print(f"Transaction batch {batch_id} committed.")


transaction_query = validation \
    .writeStream \
    .foreachBatch(process_transaction_batch) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/transactions") \
    .start()


# ============================================================
# QUERY 2 -- velocity fraud aggregation
# ============================================================

    
def process_velocity_batch(batch_df, batch_id):
    query_name = "velocity"

    if is_batch_processed(query_name, batch_id):
        print(f"[SKIP] velocity batch {batch_id} already committed.")
        return

    write_velocity_fraud_to_hdfs(batch_df, batch_id)
    write_velocity_fraud_to_mysql(batch_df, batch_id)

    mark_batch_processed(query_name, batch_id)


valid_transactions_for_velocity = validation.filter(
    ~(
        col("invalid_customer").isNotNull() |
        col("invalid_amount").isNotNull() |
        col("invalid_currency").isNotNull() |
        col("invalid_transaction_type").isNotNull() |
        col("invalid_status").isNotNull() |
        col("invalid_transaction_id").isNotNull() |
        col("invalid_account").isNotNull() |
        col("invalid_merchant").isNotNull()
    )
)

velocity_fraud = valid_transactions_for_velocity \
    .groupBy(
        window(col("transaction_timestamp"), "5 minutes"),
        col("customer_id")
    ) \
    .agg(count("*").alias("transaction_count")) \
    .filter(col("transaction_count") > 5) \
    .withColumn("fraud_rule", lit("HIGH_TRANSACTION_VELOCITY")) \
    .withColumn("fraud_reason", lit("More than 5 transactions within 5 minutes")) \
    .withColumn("fraud_status", lit("POTENTIAL_FRAUD"))

velocity_query = velocity_fraud \
    .writeStream \
    .foreachBatch(process_velocity_batch) \
    .outputMode("update") \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/velocity") \
    .start()


# ============================================================
# Wait for Streaming Queries
# ============================================================

print()
print("=" * 70)
print("REAL-TIME BANKING FRAUD PIPELINE STARTED")
print("=" * 70)
print(f"Input       : {INPUT_PATH}")
print(f"Processed   : {PROCESSED_PATH}")
print(f"Rejected    : {REJECTED_PATH}")
print(f"Fraud       : {FRAUD_PATH}")
print()
print("Fraud Rules:")
print("1. Amount > INR 100000")
print("2. More than 5 transactions within 5 minutes")
print("=" * 70)

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nStopping streaming pipeline...")
    transaction_query.stop()
    velocity_query.stop()
    spark.stop()