# Real-Time Banking Transaction & Fraud Analytics Pipeline

A hands-on real-time banking transaction and fraud analytics pipeline built using Python, RabbitMQ, HDFS, PySpark Structured Streaming, MySQL, SQL, and Streamlit.

## Architecture

```text
Python Transaction Generator
            |
            v
     RabbitMQ Producer
            |
            v
      RabbitMQ Queue
   banking_transactions
            |
            v
     RabbitMQ Consumer
            |
            v
           HDFS
            |
            v
PySpark Structured Streaming
            |
       +----+----+
       |         |
       v         v
 Validation   Fraud Detection
                  |
           +------+------+
           |             |
           v             v
       High Value   High Velocity
           |             |
           +------+------+
                  |
                  v
                MySQL
                  |
                  v
        Streamlit Dashboard
```

## Technologies

* Python
* RabbitMQ
* HDFS
* Apache Spark
* PySpark Structured Streaming
* MySQL
* SQL
* Streamlit
* Pandas
* Plotly
* Docker
* Linux

## Project Overview

The pipeline simulates banking transaction events using Python. Transactions are published as JSON messages to RabbitMQ and consumed by a downstream service.

The consumed transaction data is stored in HDFS and processed using PySpark Structured Streaming.

The Spark processing layer performs data validation, duplicate handling, transformation, and fraud detection.

Fraud alerts are stored in MySQL and analyzed through SQL views. A Streamlit dashboard provides interactive fraud and transaction analytics.

## Data Flow

1. Python generates simulated banking transactions.
2. RabbitMQ Producer publishes transaction messages.
3. RabbitMQ stores messages in the `banking_transactions` queue.
4. RabbitMQ Consumer reads the messages.
5. Transaction data is written to HDFS.
6. PySpark Structured Streaming processes the incoming data.
7. Data validation and duplicate handling are performed.
8. Fraud detection rules are applied.
9. Processed transactions and fraud alerts are stored in MySQL.
10. SQL views provide analytical summaries.
11. Streamlit displays the results through an interactive dashboard.

## Fraud Detection Rules

### 1. High Value Transaction

Transactions with an amount greater than ₹100,000 are flagged as:

`HIGH_VALUE_TRANSACTION`

### 2. High Transaction Velocity

When a customer performs more than 5 transactions within a 5-minute window, the activity is flagged as:

`HIGH_TRANSACTION_VELOCITY`

## Data Validation

The pipeline validates incoming transactions for:

* Required fields
* Null values
* Valid transaction types
* Valid transaction status
* INR currency
* Duplicate transaction IDs

Invalid records are separated from valid records for further analysis.

## Project Structure

```text
banking-data-pipeline/
│
├── dashboard/
│   ├── app.py
│   ├── db.py
│   └── requirements.txt
│
├── data/
│   └── input/
│
├── producer/
│   └── app/
│       ├── transaction_generator.py
│       ├── rabbitmq_producer.py
│       ├── rabbitmq_consumer.py
│       └── rabbitmq_to_hdfs.py
│
├── spark/
│   └── app/
│       ├── spark_test.py
│       ├── read_transactions.py
│       ├── streaming_transactions.py
│       └── 05_duplicate_handling.py
│
├── sql/
│   └── views.sql
│
├── docs/
│   ├── architecture.png
│   └── dashboard-overview.png
│
└── README.md
```

## Database

The pipeline uses MySQL to store processed transactions and fraud alerts.

Key tables include:

* `transactions`
* `fraud_alerts`
* `processed_batches`

SQL views are used for:

* Fraud summary
* Customer-level fraud analysis
* Daily fraud trends

## Dashboard

The Streamlit dashboard provides interactive analysis of the processed fraud data.

It includes:

* Customer filtering
* Fraud rule filtering
* Date filtering
* Fraud alert statistics
* Customer-level analysis
* Daily fraud trends
* Recent fraud alerts

## Running the Project

The project requires Python, Docker, RabbitMQ, HDFS, Apache Spark, and MySQL.

The main execution flow is:

```text
Start RabbitMQ
      ↓
Generate Transactions
      ↓
Run RabbitMQ Producer
      ↓
Run Consumer / HDFS Ingestion
      ↓
Start PySpark Streaming
      ↓
Process Transactions
      ↓
Store Results in MySQL
      ↓
Run Streamlit Dashboard
```

## Project Purpose

This project was built as a hands-on Data Engineering project to practice real-time data ingestion, message-based processing, distributed storage, stream processing, data quality, SQL analytics, and dashboard development.

## Author

**Ananth M.**

Data Engineer | Python | SQL | PySpark | AWS | Hadoop
