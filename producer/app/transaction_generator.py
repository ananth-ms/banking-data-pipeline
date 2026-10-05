import random
import uuid
from datetime import datetime


CUSTOMER_ACCOUNTS = {
    "CUST0001": "ACC0001",
    "CUST0002": "ACC0002",
    "CUST0003": "ACC0003",
    "CUST0004": "ACC0004",
    "CUST0005": "ACC0005",
    "CUST0006": "ACC0006",
    "CUST0007": "ACC0007",
    "CUST0008": "ACC0008",
    "CUST0009": "ACC0009",
    "CUST0010": "ACC0010"
}

TRANSACTION_TYPES = [
    "UPI",
    "ATM",
    "CARD",
    "NET_BANKING",
    "IMPS",
    "NEFT"
]

MERCHANTS = [
    "Amazon",
    "Flipkart",
    "Swiggy",
    "Zomato",
    "Myntra",
    "Reliance",
    "Uber",
    "IRCTC"
]

LOCATIONS = [
    "Chennai",
    "Bangalore",
    "Mumbai",
    "Delhi",
    "Hyderabad",
    "Pune"
]

STATUSES = [
    "SUCCESS",
    "SUCCESS",
    "SUCCESS",
    "SUCCESS",
    "FAILED"
]


def generate_transaction():

    customer_id = random.choice(
        list(CUSTOMER_ACCOUNTS.keys())
    )

    account_id = CUSTOMER_ACCOUNTS[customer_id]

    transaction = {
        "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
        "customer_id": customer_id,
        "account_id": account_id,
        "amount": round(random.uniform(100, 200000), 2),
        "currency": "INR",
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "merchant": random.choice(MERCHANTS),
        "location": random.choice(LOCATIONS),
        "timestamp": datetime.now().isoformat(),
        "status": random.choice(STATUSES)
    }

    # Occasionally generate invalid data
    if random.random() < 0.20:
x
        invalid_type = random.choice([
            "missing_customer",
            "negative_amount",
            "invalid_status",
            "invalid_transaction_type"
        ])

        if invalid_type == "missing_customer":
            transaction["customer_id"] = None

        elif invalid_type == "negative_amount":
            transaction["amount"] = -5000

        elif invalid_type == "invalid_status":
            transaction["status"] = "UNKNOWN"

        elif invalid_type == "invalid_transaction_type":
            transaction["transaction_type"] = "INVALID"

    return transaction

if __name__ == "__main__":

    transaction = generate_transaction()

    print(transaction)
