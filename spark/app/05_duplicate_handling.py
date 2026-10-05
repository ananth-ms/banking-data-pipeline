from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import sys


# ============================================================
# 1. Create Spark Session
# ============================================================

spark = SparkSession.builder \
    .appName("BankingDuplicateHandling") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. Input Path
# ============================================================

if len(sys.argv) > 1:
    input_path = sys.argv[1]
else:
    input_path = (
        "hdfs://localhost:9000/"
        "ananth/realtime-banking-data-pipeline/"
        "data/input/transactions.json"
    )


# ============================================================
# 3. Duplicate Output Path
# ============================================================

duplicate_path = (
    "hdfs://localhost:9000/"
    "ananth/realtime-banking-data-pipeline/"
    "data/duplicates/"
)


# ============================================================
# 4. Read Transactions
# ============================================================

print("=" * 70)
print("READING TRANSACTIONS")
print("=" * 70)

print(f"Input path: {input_path}")

df = spark.read.json(input_path)

total_records = df.count()

print(f"Total input records: {total_records}")


# ============================================================
# 5. Display Input
# ============================================================

print("=" * 70)
print("INPUT DATA")
print("=" * 70)

df.select(
    "transaction_id",
    "customer_id",
    "amount",
    "transaction_type",
    "timestamp",
    "status"
).show(truncate=False)


# ============================================================
# 6. Find Duplicate Transaction IDs
# ============================================================

print("=" * 70)
print("CHECKING FOR DUPLICATES")
print("=" * 70)

duplicate_ids_df = df \
    .groupBy("transaction_id") \
    .agg(
        count("*").alias("record_count")
    ) \
    .filter(
        col("record_count") > 1
    )

duplicate_transaction_ids = duplicate_ids_df.count()

print(
    f"Number of duplicate transaction IDs: "
    f"{duplicate_transaction_ids}"
)

duplicate_ids_df.show(truncate=False)


# ============================================================
# 7. Extract Duplicate Records
# ============================================================

duplicate_records_df = df.join(
    duplicate_ids_df.select("transaction_id"),
    on="transaction_id",
    how="inner"
)

duplicate_records = duplicate_records_df.count()

print("=" * 70)
print("DUPLICATE RECORDS")
print("=" * 70)

duplicate_records_df.select(
    "transaction_id",
    "customer_id",
    "account_id",
    "amount",
    "transaction_type",
    "merchant",
    "location",
    "timestamp",
    "status"
).show(truncate=False)


# ============================================================
# 8. Remove Duplicates
# ============================================================

unique_df = df.dropDuplicates(
    ["transaction_id"]
)

unique_records = unique_df.count()

print("=" * 70)
print("UNIQUE TRANSACTIONS")
print("=" * 70)

unique_df.select(
    "transaction_id",
    "customer_id",
    "account_id",
    "amount",
    "transaction_type",
    "merchant",
    "location",
    "timestamp",
    "status"
).show(truncate=False)


# ============================================================
# 9. Write Duplicate Records to HDFS
# ============================================================

print("=" * 70)
print("WRITING DUPLICATES TO HDFS")
print("=" * 70)

if duplicate_records > 0:

    duplicate_records_df.write \
        .mode("overwrite") \
        .parquet(duplicate_path)

    print(
        f"Duplicate records written to: "
        f"{duplicate_path}"
    )

else:

    print("No duplicate records found.")


# ============================================================
# 10. Final Summary
# ============================================================

print("=" * 70)
print("DUPLICATE HANDLING SUMMARY")
print("=" * 70)

print(f"Total input records       : {total_records}")
print(f"Unique records            : {unique_records}")
print(f"Duplicate records         : {duplicate_records}")
print(f"Duplicate transaction IDs : {duplicate_transaction_ids}")

print("=" * 70)
print("DUPLICATE HANDLING COMPLETED")
print("=" * 70)


# ============================================================
# 11. Stop Spark
# ============================================================

spark.stop()
