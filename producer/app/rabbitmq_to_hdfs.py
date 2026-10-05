import json
import os
import time
import uuid
import pika


# ============================================================
# RabbitMQ Configuration
# ============================================================

RABBITMQ_HOST = "localhost"

QUEUE_NAME = "banking_transactions"

# Retry queues
RETRY_QUEUE_1 = "banking_transactions_retry_1"
RETRY_QUEUE_2 = "banking_transactions_retry_2"

# Dead Letter Queue
DLQ_NAME = "banking_transactions_dlq"

# Exchanges
RETRY_EXCHANGE = "banking_retry_exchange"
DLQ_EXCHANGE = "banking_dlq_exchange"


# ============================================================
# HDFS Configuration
# ============================================================

HDFS_STREAM_INPUT = (
    "/ananth/realtime-banking-data-pipeline/"
    "data/stream_input"
)


# ============================================================
# Batch Configuration
# ============================================================

BATCH_SIZE = 5
BATCH_TIMEOUT = 5

# Retry delays in milliseconds
RETRY_1_DELAY = 5000
RETRY_2_DELAY = 10000


# ============================================================
# RabbitMQ Setup
# ============================================================

def setup_rabbitmq(channel):

    # --------------------------------------------------------
    # Main Queue
    # --------------------------------------------------------

    channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True
    )

    # --------------------------------------------------------
    # Retry Exchange
    # --------------------------------------------------------

    channel.exchange_declare(
        exchange=RETRY_EXCHANGE,
        exchange_type="direct",
        durable=True
    )

    # --------------------------------------------------------
    # Retry Queue 1
    # --------------------------------------------------------

    channel.queue_declare(
        queue=RETRY_QUEUE_1,
        durable=True,
        arguments={
            "x-message-ttl": RETRY_1_DELAY,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": QUEUE_NAME
        }
    )

    channel.queue_bind(
        exchange=RETRY_EXCHANGE,
        queue=RETRY_QUEUE_1,
        routing_key=RETRY_QUEUE_1
    )

    # --------------------------------------------------------
    # Retry Queue 2
    # --------------------------------------------------------

    channel.queue_declare(
        queue=RETRY_QUEUE_2,
        durable=True,
        arguments={
            "x-message-ttl": RETRY_2_DELAY,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": QUEUE_NAME
        }
    )

    channel.queue_bind(
        exchange=RETRY_EXCHANGE,
        queue=RETRY_QUEUE_2,
        routing_key=RETRY_QUEUE_2
    )

    # --------------------------------------------------------
    # DLQ Exchange
    # --------------------------------------------------------

    channel.exchange_declare(
        exchange=DLQ_EXCHANGE,
        exchange_type="direct",
        durable=True
    )

    # --------------------------------------------------------
    # Dead Letter Queue
    # --------------------------------------------------------

    channel.queue_declare(
        queue=DLQ_NAME,
        durable=True
    )

    channel.queue_bind(
        exchange=DLQ_EXCHANGE,
        queue=DLQ_NAME,
        routing_key=DLQ_NAME
    )

    print("RabbitMQ queues and exchanges configured.")


# ============================================================
# Send Message to Retry Queue
# ============================================================

def send_to_retry_queue(
    channel,
    transaction,
    retry_count
):

    message = json.dumps(transaction)

    if retry_count == 1:

        target_queue = RETRY_QUEUE_1

        print(
            f"Sending transaction "
            f"{transaction.get('transaction_id')} "
            f"to RETRY #1"
        )

    else:

        target_queue = RETRY_QUEUE_2

        print(
            f"Sending transaction "
            f"{transaction.get('transaction_id')} "
            f"to RETRY #2"
        )

    channel.basic_publish(
        exchange=RETRY_EXCHANGE,
        routing_key=target_queue,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,
            headers={
                "retry_count": retry_count
            }
        )
    )


# ============================================================
# Send Message to DLQ
# ============================================================

def send_to_dlq(
    channel,
    transaction,
    reason
):

    message = json.dumps(transaction)

    print(
        f"Sending transaction "
        f"{transaction.get('transaction_id')} "
        f"to DLQ"
    )

    channel.basic_publish(
        exchange=DLQ_EXCHANGE,
        routing_key=DLQ_NAME,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,
            headers={
                "dlq_reason": reason
            }
        )
    )


# ============================================================
# Write Batch to HDFS
# ============================================================

def write_batch_to_hdfs(transactions):

    if not transactions:
        return True

    file_name = (
        f"rabbitmq_batch_{int(time.time())}_"
        f"{uuid.uuid4().hex[:8]}.json"
    )

    local_file = f"/tmp/{file_name}"

    # --------------------------------------------------------
    # Create local JSONL file
    # --------------------------------------------------------

    try:

        with open(local_file, "w") as file:

            for transaction in transactions:

                file.write(
                    json.dumps(transaction) + "\n"
                )

        print(
            f"Created local batch: {local_file}"
        )

    except Exception as e:

        print(
            f"ERROR creating local batch: {e}"
        )

        return False

    # --------------------------------------------------------
    # Upload to HDFS
    # --------------------------------------------------------

    command = (
        f"hdfs dfs -put "
        f"{local_file} "
        f"{HDFS_STREAM_INPUT}/{file_name}"
    )

    print(
        f"Writing batch to HDFS: {file_name}"
    )

    result = os.system(command)

    # --------------------------------------------------------
    # HDFS Success
    # --------------------------------------------------------

    if result == 0:

        print(
            f"Successfully written "
            f"{len(transactions)} "
            f"transactions to HDFS"
        )

        try:
            os.remove(local_file)
        except Exception:
            pass

        return True

    # --------------------------------------------------------
    # HDFS Failure
    # --------------------------------------------------------

    print(
        "ERROR: Failed to write batch to HDFS"
    )

    print(
        f"Local file retained: {local_file}"
    )

    return False


# ============================================================
# Process Batch
# ============================================================

def process_batch(
    channel,
    batch_transactions,
    batch_delivery_tags
):

    if not batch_transactions:
        return

    print()
    print("=" * 70)
    print(
        f"PROCESSING BATCH: "
        f"{len(batch_transactions)} transactions"
    )
    print("=" * 70)

    # --------------------------------------------------------
    # Try writing to HDFS
    # --------------------------------------------------------

    success = write_batch_to_hdfs(
        batch_transactions
    )

    # --------------------------------------------------------
    # SUCCESS
    # --------------------------------------------------------

    if success:

        print(
            "HDFS write successful."
        )

        print(
            "Acknowledging RabbitMQ messages..."
        )

        for delivery_tag in batch_delivery_tags:

            channel.basic_ack(
                delivery_tag=delivery_tag
            )

        print(
            "All messages acknowledged."
        )

        print("=" * 70)
        print()

        return

    # --------------------------------------------------------
    # FAILURE
    # --------------------------------------------------------

    print(
        "HDFS write failed."
    )

    print(
        "Starting retry processing..."
    )

    for transaction in batch_transactions:

        transaction_id = transaction.get(
            "transaction_id",
            "UNKNOWN"
        )

        print(
            f"Processing retry for "
            f"{transaction_id}"
        )

        # ----------------------------------------------------
        # Current retry count
        # ----------------------------------------------------

        # Transactions entering this function normally
        # start with retry count 0.
        #
        # We determine retry count from the transaction
        # metadata when available.

        retry_count = transaction.get(
            "_retry_count",
            0
        )

        # ----------------------------------------------------
        # Retry #1
        # ----------------------------------------------------

        if retry_count == 0:

            transaction["_retry_count"] = 1

            send_to_retry_queue(
                channel,
                transaction,
                1
            )

        # ----------------------------------------------------
        # Retry #2
        # ----------------------------------------------------

        elif retry_count == 1:

            transaction["_retry_count"] = 2

            send_to_retry_queue(
                channel,
                transaction,
                2
            )

        # ----------------------------------------------------
        # Maximum retries reached
        # ----------------------------------------------------

        else:

            send_to_dlq(
                channel,
                transaction,
                "HDFS_WRITE_FAILED_AFTER_RETRIES"
            )

    # --------------------------------------------------------
    # ACK original messages after safely publishing them
    # --------------------------------------------------------

    for delivery_tag in batch_delivery_tags:

        channel.basic_ack(
            delivery_tag=delivery_tag
        )

    print(
        "Original messages acknowledged "
        "after retry/DLQ routing."
    )

    print("=" * 70)
    print()


# ============================================================
# Main
# ============================================================

def main():

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST
        )
    )

    channel = connection.channel()

    # --------------------------------------------------------
    # Setup RabbitMQ
    # --------------------------------------------------------

    setup_rabbitmq(channel)

    # --------------------------------------------------------
    # Limit unacknowledged messages
    # --------------------------------------------------------

    channel.basic_qos(
        prefetch_count=BATCH_SIZE
    )

    print("=" * 70)
    print("RABBITMQ → HDFS BRIDGE")
    print("=" * 70)
    print(
        f"RabbitMQ host : {RABBITMQ_HOST}"
    )
    print(
        f"Main queue    : {QUEUE_NAME}"
    )
    print(
        f"Retry #1      : {RETRY_QUEUE_1}"
    )
    print(
        f"Retry #2      : {RETRY_QUEUE_2}"
    )
    print(
        f"DLQ           : {DLQ_NAME}"
    )
    print(
        f"HDFS input    : {HDFS_STREAM_INPUT}"
    )
    print(
        f"Batch size    : {BATCH_SIZE}"
    )
    print("=" * 70)

    # --------------------------------------------------------
    # Batch storage
    # --------------------------------------------------------

    transactions = []
    delivery_tags = []

    last_batch_time = time.time()

    try:

        while True:

            method_frame, header_frame, body = (
                channel.basic_get(
                    queue=QUEUE_NAME,
                    auto_ack=False
                )
            )

            # ------------------------------------------------
            # Message received
            # ------------------------------------------------

            if method_frame:

                try:

                    transaction = json.loads(
                        body.decode("utf-8")
                    )

                    # ----------------------------------------
                    # Read retry count
                    # ----------------------------------------

                    retry_count = 0

                    if header_frame.headers:

                        retry_count = header_frame.headers.get(
                            "retry_count",
                            0
                        )

                    transaction["_retry_count"] = retry_count

                    transactions.append(
                        transaction
                    )

                    delivery_tags.append(
                        method_frame.delivery_tag
                    )

                    print(
                        f"Received: "
                        f"{transaction.get('transaction_id')} "
                        f"| retry={retry_count}"
                    )

                except Exception as e:

                    print(
                        f"ERROR processing message: {e}"
                    )

                    # ----------------------------------------
                    # Malformed message
                    # Send directly to DLQ
                    # ----------------------------------------

                    try:

                        bad_message = {
                            "raw_message": body.decode(
                                "utf-8",
                                errors="replace"
                            )
                        }

                        send_to_dlq(
                            channel,
                            bad_message,
                            "INVALID_JSON"
                        )

                        channel.basic_ack(
                            delivery_tag=
                            method_frame.delivery_tag
                        )

                        print(
                            "Malformed message moved to DLQ."
                        )

                    except Exception as dlq_error:

                        print(
                            f"ERROR sending malformed "
                            f"message to DLQ: {dlq_error}"
                        )

                        channel.basic_nack(
                            delivery_tag=
                            method_frame.delivery_tag,
                            requeue=True
                        )

            # ------------------------------------------------
            # Check whether batch should be processed
            # ------------------------------------------------

            current_time = time.time()

            batch_ready = (
                len(transactions) >= BATCH_SIZE
                or
                (
                    transactions
                    and
                    current_time - last_batch_time
                    >= BATCH_TIMEOUT
                )
            )

            if batch_ready:

                process_batch(
                    channel,
                    transactions,
                    delivery_tags
                )

                transactions = []
                delivery_tags = []

                last_batch_time = time.time()

            # ------------------------------------------------
            # No message available
            # ------------------------------------------------

            if not method_frame:

                time.sleep(1)

    except KeyboardInterrupt:

        print(
            "\nStopping RabbitMQ → HDFS bridge..."
        )

        # ----------------------------------------------------
        # Process remaining messages
        # ----------------------------------------------------

        if transactions:

            print(
                f"Processing remaining "
                f"{len(transactions)} transactions..."
            )

            process_batch(
                channel,
                transactions,
                delivery_tags
            )

    finally:

        connection.close()

        print(
            "Bridge stopped."
        )


# ============================================================
# Application Entry Point
# ============================================================

if __name__ == "__main__":

    main()
