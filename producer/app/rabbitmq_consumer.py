import pika


RABBITMQ_HOST = "localhost"
QUEUE_NAME = "banking_transactions"


def callback(ch, method, properties, body):

    print("Received transaction:")
    print(body.decode())
    print("-" * 60)

    ch.basic_ack(
        delivery_tag=method.delivery_tag
    )


def start_consumer():

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )

    channel = connection.channel()

    channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True
    )

    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=callback
    )

    print("Connected to RabbitMQ")
    print(f"Waiting for messages from: {QUEUE_NAME}")

    try:

        channel.start_consuming()

    except KeyboardInterrupt:

        print("\nStopping consumer...")

        channel.stop_consuming()

    finally:

        connection.close()


if __name__ == "__main__":
    start_consumer()
