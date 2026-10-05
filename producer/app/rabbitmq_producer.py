import json
import pika
import time

from transaction_generator import generate_transaction


RABBITMQ_HOST = "localhost"
QUEUE_NAME = "banking_transactions"


def connect_to_rabbitmq():

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )

    channel = connection.channel()

    channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True
    )

    return connection, channel


def start_producer():

    connection, channel = connect_to_rabbitmq()

    print("Connected to RabbitMQ")
    print(f"Publishing transactions to queue: {QUEUE_NAME}")

    try:

        while True:

            transaction = generate_transaction()

            message = json.dumps(transaction)

            channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2
                )
            )

            print(f"Published: {message}")

            time.sleep(2)

    except KeyboardInterrupt:

        print("\nStopping producer...")

    finally:

        connection.close()


if __name__ == "__main__":
    start_producer()
