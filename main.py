from faker import Faker
from confluent_kafka import SerializingProducer
from datetime import datetime, UTC
import json
import random
import time


fake = Faker()

def generate_sales_transactions():
    user = fake.simple_profile()

    return{
        "transactionId": fake.uuid4(),
        "productId": random.choice(['product1', 'product2', 'product3', 'product4', 'product5', 'product6']),
        "productName" : random.choice(['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Smartwatch', 'Camera']),
        "productCategory": random.choice(['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Toys', 'Sports']),
        "productPrice": round(random.uniform(10,1000),2),
        "productQuantity": random.randint(1,10),
        "productBrand": random.choice(['apple', 'samsung', 'sony', 'lg', 'dell', 'hp']),
        "currency": random.choice(['USD', 'EGP']),
        "customerId": user['username'],
        'transactionDate': datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "paymentMethod": random.choice(['credit_card', 'debit_card', 'online_transfer', 'bank_transfer', 'cash_on_delivery'])
      
    }                  

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for Transaction {msg.key()}: {err}")
    else:
        print(f"Transaction {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def main():
    topic = 'financial_transactions'
    producer = SerializingProducer({
        'bootstrap.servers' : 'localhost:9092'
    })

    curr_time = datetime.now()

    while (datetime.now() - curr_time).seconds < 120 :
        try :    
            transaction = generate_sales_transactions()
            transaction['totalAmount'] = transaction['productPrice'] * transaction['productQuantity']

            print(transaction)

            producer.produce(
                topic=topic,
                key=transaction['transactionId'],
                value=json.dumps(transaction),
                on_delivery = delivery_report
            )

            producer.poll(0)
            time.sleep(5)
        except BufferError as e:
            print(f"Local producer queue is full ({len(producer)} messages awaiting delivery): {e}")
       # finally :
          #  producer.flush()


if __name__ == "__main__":
    main()
