from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    concat_ws,
    array,
    lit,
    to_timestamp,
    year,
    month,
    dayofmonth,
    count,
    window
)

# ============================================================
# 1. Spark Session
# ============================================================

spark = SparkSession.builder \
    .appName("BankingTransactionFraudDetection") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. Paths
# ============================================================

input_path = (
    "hdfs://localhost:9000/"
    "ananth/realtime-banking-data-pipeline/"
    "data/input/transactions.json"
)

processed_path = (
    "hdfs://localhost:9000/"
    "ananth/realtime-banking-data-pipeline/"
    "data/processed/"
)

rejected_path = (
    "hdfs://localhost:9000/"
    "ananth/realtime-banking-data-pipeline/"
    "data/rejected/"
)

fraud_path = (
    "hdfs://localhost:9000/"
    "ananth/realtime-banking-data-pipeline/"
    "data/fraud/"
)


# ============================================================
# 3. MySQL Configuration
# ============================================================

mysql_url = "jdbc:mysql://localhost:3306/banking_db"

mysql_properties = {
    "user": "ananth",
    "password": "Ananth@2003",
    "driver": "com.mysql.cj.jdbc.Driver"
}


# ============================================================
# 4. Read Transactions from HDFS
# ============================================================

print("=" * 70)
print("READING TRANSACTIONS FROM HDFS")
print("=" * 70)

df = spark.read.json(input_path)

df = df.withColumn(
    "transaction_timestamp",
    to_timestamp(col("timestamp"))
)

print("Input transactions:")
df.show(truncate=False)


# ============================================================
# 5. Validation Rules
# ============================================================

print("=" * 70)
print("VALIDATING TRANSACTIONS")
print("=" * 70)

validation_df = df.withColumn(
    "rejection_reason",
    concat_ws(
        ",",
        array(
            when(
                ~(
                    col("customer_id").isNotNull()
                    & (col("customer_id") != "")
                ),
                "MISSING_CUSTOMER_ID"
            ),

            when(
                ~(col("amount").isNotNull() & (col("amount") > 0)),
                "INVALID_AMOUNT"
            ),

            when(
                ~col("status").isin("SUCCESS", "FAILED"),
                "INVALID_STATUS"
            ),

            when(
                ~col("transaction_type").isin(
                    "UPI",
                    "ATM",
                    "CARD",
                    "NET_BANKING",
                    "IMPS",
                    "NEFT"
                ),
                "INVALID_TRANSACTION_TYPE"
            ),

            when(
                ~(
                    col("currency").isNotNull()
                    & (col("currency") == "INR")
                ),
                "INVALID_CURRENCY"
            ),

            when(
                ~(
                    col("transaction_id").isNotNull()
                    & (col("transaction_id") != "")
                ),
                "MISSING_TRANSACTION_ID"
            ),

            when(
                ~(
                    col("account_id").isNotNull()
                    & (col("account_id") != "")
                ),
                "MISSING_ACCOUNT_ID"
            ),

            when(
                ~(
                    col("merchant").isNotNull()
                    & (col("merchant") != "")
                ),
                "MISSING_MERCHANT"
            )
        )
    )
)


# ============================================================
# 6. Split Valid and Rejected Transactions
# ============================================================

valid_df = validation_df.filter(
    col("rejection_reason") == ""
)

rejected_df = validation_df.filter(
    col("rejection_reason") != ""
)


# ============================================================
# 7. Add Partition Columns
# ============================================================

valid_df = valid_df.withColumn(
    "year",
    year(col("transaction_timestamp"))
).withColumn(
    "month",
    month(col("transaction_timestamp"))
).withColumn(
    "day",
    dayofmonth(col("transaction_timestamp"))
)

rejected_df = rejected_df.withColumn(
    "year",
    year(col("transaction_timestamp"))
).withColumn(
    "month",
    month(col("transaction_timestamp"))
).withColumn(
    "day",
    dayofmonth(col("transaction_timestamp"))
)


# ============================================================
# 8. Display Validation Results
# ============================================================

print("=" * 70)
print("VALID TRANSACTIONS")
print("=" * 70)

valid_df.select(
    "transaction_id",
    "customer_id",
    "account_id",
    "amount",
    "transaction_type",
    "status"
).show(truncate=False)


print("=" * 70)
print("REJECTED TRANSACTIONS")
print("=" * 70)

rejected_df.select(
    "transaction_id",
    "customer_id",
    "amount",
    "transaction_type",
    "status",
    "rejection_reason"
).show(truncate=False)


# ============================================================
# 9. Write Valid Transactions to HDFS
# ============================================================

print("=" * 70)
print("WRITING VALID TRANSACTIONS TO HDFS")
print("=" * 70)

valid_df.write \
    .mode("append") \
    .partitionBy("year", "month", "day") \
    .parquet(processed_path)

print("Valid transactions written to HDFS.")


# ============================================================
# 10. Write Rejected Transactions to HDFS
# ============================================================

print("=" * 70)
print("WRITING REJECTED TRANSACTIONS TO HDFS")
print("=" * 70)

rejected_df.write \
    .mode("append") \
    .partitionBy("year", "month", "day") \
    .parquet(rejected_path)

print("Rejected transactions written to HDFS.")


# ============================================================
# 11. High Value Fraud Detection
# ============================================================

print("=" * 70)
print("CHECKING HIGH VALUE TRANSACTIONS")
print("=" * 70)

high_value_fraud_df = valid_df.filter(
    col("amount") > 100000
).withColumn(
    "fraud_rule",
    lit("HIGH_VALUE_TRANSACTION")
).withColumn(
    "fraud_reason",
    lit("Transaction amount exceeds INR 100000")
).withColumn(
    "fraud_status",
    lit("POTENTIAL_FRAUD")
)

high_value_count = high_value_fraud_df.count()

print(f"High-value fraud alerts: {high_value_count}")

high_value_fraud_df.select(
    "transaction_id",
    "customer_id",
    "amount",
    "transaction_type",
    "merchant",
    "location",
    "fraud_rule",
    "fraud_reason",
    "fraud_status"
).show(truncate=False)


# ============================================================
# 12. Write High Value Fraud to HDFS
# ============================================================

high_value_fraud_df.write \
    .mode("append") \
    .partitionBy("year", "month", "day") \
    .parquet(fraud_path + "high_value/")

print("High-value fraud alerts written to HDFS.")


# ============================================================
# 13. Transaction Velocity Detection
# ============================================================

print("=" * 70)
print("CHECKING TRANSACTION VELOCITY")
print("=" * 70)

velocity_df = valid_df \
    .withWatermark(
        "transaction_timestamp",
        "10 minutes"
    ) \
    .groupBy(
        window(
            col("transaction_timestamp"),
            "5 minutes"
        ),
        col("customer_id")
    ) \
    .agg(
        count("*").alias("transaction_count")
    )

velocity_fraud_df = velocity_df.filter(
    col("transaction_count") > 5
)


# ============================================================
# 14. Add Fraud Details
# IMPORTANT:
# Keep window_start and window_end available.
# ============================================================

velocity_fraud_df = velocity_fraud_df \
    .withColumn(
        "window_start",
        col("window.start")
    ) \
    .withColumn(
        "window_end",
        col("window.end")
    ) \
    .withColumn(
        "fraud_rule",
        lit("HIGH_TRANSACTION_VELOCITY")
    ) \
    .withColumn(
        "fraud_reason",
        lit("More than 5 transactions within 5 minutes")
    ) \
    .withColumn(
        "fraud_status",
        lit("POTENTIAL_FRAUD")
    )


print("=" * 70)
print("VELOCITY FRAUD ALERTS")
print("=" * 70)

velocity_fraud_df.select(
    "customer_id",
    "window_start",
    "window_end",
    "transaction_count",
    "fraud_rule",
    "fraud_reason",
    "fraud_status"
).show(truncate=False)


velocity_count = velocity_fraud_df.count()

print(f"Velocity fraud alerts: {velocity_count}")


# ============================================================
# 15. Write Velocity Fraud to HDFS
# ============================================================

velocity_fraud_df.write \
    .mode("append") \
    .parquet(fraud_path + "velocity/")

print("Velocity fraud alerts written to HDFS.")


# ============================================================
# 16. Prepare Transactions for MySQL
# ============================================================

print("=" * 70)
print("WRITING TRANSACTIONS TO MYSQL")
print("=" * 70)

mysql_transactions_df = valid_df.select(
    "transaction_id",
    "customer_id",
    "account_id",
    "amount",
    "currency",
    "transaction_type",
    "merchant",
    "location",
    "transaction_timestamp",
    "status"
)

mysql_transactions_df.write \
    .mode("append") \
    .jdbc(
        url=mysql_url,
        table="transactions",
        properties=mysql_properties
    )

print("Transactions successfully written to MySQL.")


# ============================================================
# 17. Prepare High Value Fraud Alerts for MySQL
# ============================================================

print("=" * 70)
print("WRITING HIGH VALUE FRAUD ALERTS TO MYSQL")
print("=" * 70)

mysql_high_value_fraud_df = high_value_fraud_df.select(
    "transaction_id",
    "customer_id",
    "account_id",
    "amount",
    "currency",
    "transaction_type",
    "merchant",
    "location",
    "transaction_timestamp",
    "status",
    "fraud_rule",
    "fraud_reason",
    "fraud_status"
)

mysql_high_value_fraud_df.write \
    .mode("append") \
    .jdbc(
        url=mysql_url,
        table="fraud_alerts",
        properties=mysql_properties
    )

print("High-value fraud alerts written to MySQL.")


# ============================================================
# 18. Prepare Velocity Fraud Alerts for MySQL
# ============================================================

print("=" * 70)
print("PREPARING VELOCITY FRAUD ALERTS FOR MYSQL")
print("=" * 70)

mysql_velocity_fraud_df = velocity_fraud_df \
    .withColumn(
        "transaction_id",
        lit(None).cast("string")
    ) \
    .withColumn(
        "account_id",
        lit(None).cast("string")
    ) \
    .withColumn(
        "amount",
        lit(None).cast("double")
    ) \
    .withColumn(
        "currency",
        lit(None).cast("string")
    ) \
    .withColumn(
        "transaction_type",
        lit(None).cast("string")
    ) \
    .withColumn(
        "merchant",
        lit(None).cast("string")
    ) \
    .withColumn(
        "location",
        lit(None).cast("string")
    ) \
    .withColumn(
        "status",
        lit("SUCCESS")
    ) \
    .withColumn(
        "transaction_timestamp",
        col("window_start")
    )


# ============================================================
# 19. Select Exact MySQL Columns
# ============================================================

mysql_velocity_fraud_df = mysql_velocity_fraud_df.select(
    "transaction_id",
    "customer_id",
    "account_id",
    "amount",
    "currency",
    "transaction_type",
    "merchant",
    "location",
    "transaction_timestamp",
    "status",
    "fraud_rule",
    "fraud_reason",
    "fraud_status"
)


print("=" * 70)
print("VELOCITY FRAUD ALERTS READY FOR MYSQL")
print("=" * 70)

mysql_velocity_fraud_df.show(truncate=False)


# ============================================================
# 20. Write Velocity Fraud Alerts to MySQL
# ============================================================

if mysql_velocity_fraud_df.count() > 0:

    mysql_velocity_fraud_df.write \
        .mode("append") \
        .jdbc(
            url=mysql_url,
            table="fraud_alerts",
            properties=mysql_properties
        )

    print("Velocity fraud alerts successfully written to MySQL.")

else:

    print("No velocity fraud alerts to write.")


# ============================================================
# 21. Final Summary
# ============================================================

total_transactions = df.count()
valid_transactions = valid_df.count()
rejected_transactions = rejected_df.count()

print("=" * 70)
print("FINAL PIPELINE SUMMARY")
print("=" * 70)

print(f"Total Transactions       : {total_transactions}")
print(f"Valid Transactions      : {valid_transactions}")
print(f"Rejected Transactions   : {rejected_transactions}")
print(f"High Value Fraud Alerts : {high_value_count}")
print(f"Velocity Fraud Alerts   : {velocity_count}")

print("=" * 70)
print("PIPELINE COMPLETED")
print("=" * 70)


# ============================================================
# 22. Stop Spark
# ============================================================

spark.stop()
