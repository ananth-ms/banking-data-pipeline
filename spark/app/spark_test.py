from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BankingTransactionPipeline") \
    .master("local[*]") \
    .getOrCreate()

print("Spark Session Created Successfully")

data = [
    ("TXN001", "CUST001", 5000.0),
    ("TXN002", "CUST002", 15000.0),
    ("TXN003", "CUST003", 250000.0)
]

columns = ["transaction_id", "customer_id", "amount"]

df = spark.createDataFrame(data, columns)

df.show()

spark.stop()
