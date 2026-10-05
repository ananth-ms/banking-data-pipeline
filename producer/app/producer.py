import json
import time

from transaction_generator import generate_transaction


def start_generator():

    print("Starting banking transaction generator...")

    while True:

        transaction = generate_transaction()

        print(json.dumps(transaction, indent=2))

        print("-" * 60)

        time.sleep(2)


if __name__ == "__main__":
    start_generator()
