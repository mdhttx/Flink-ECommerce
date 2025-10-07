''' from faker import Faker
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

    while (datetime.now() - curr_time).seconds < 3600 :
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
'''

from faker import Faker
from confluent_kafka import SerializingProducer
from datetime import datetime, UTC
import json
import random
import time


fake = Faker()

def generate_sales_transactions():
    user = fake.simple_profile()
    
    product_price = round(random.uniform(10, 1000), 2)
    product_quantity = random.randint(1, 10)
    total_amount = product_price * product_quantity
    
    # Shipping and tax calculations
    shipping_cost = round(random.uniform(5, 50), 2) if random.random() > 0.3 else 0  # 70% have shipping cost
    tax_rate = random.choice([0.05, 0.08, 0.10, 0.14])  # Different tax rates
    tax_amount = round(total_amount * tax_rate, 2)
    discount_percentage = random.choice([0, 5, 10, 15, 20]) if random.random() > 0.6 else 0
    discount_amount = round(total_amount * (discount_percentage / 100), 2)
    final_amount = round(total_amount + shipping_cost + tax_amount - discount_amount, 2)

    return{
        "transactionId": fake.uuid4(),
        "productId": random.choice(['product1', 'product2', 'product3', 'product4', 'product5', 'product6']),
        "productName" : random.choice(['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Smartwatch', 'Camera']),
        "productCategory": random.choice(['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Toys', 'Sports']),
        "productPrice": product_price,
        "productQuantity": product_quantity,
        "productBrand": random.choice(['apple', 'samsung', 'sony', 'lg', 'dell', 'hp']),
        "totalAmount": total_amount,
        "currency": random.choice(['USD', 'EGP']),
        "customerId": user['username'],
        'transactionDate': datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "paymentMethod": random.choice(['credit_card', 'debit_card', 'online_transfer', 'bank_transfer', 'cash_on_delivery']),
        
        # Additional e-commerce fields
        "customerEmail": user['mail'],
        "shippingAddress": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state(),
            "zipCode": fake.zipcode(),
            "country": random.choice(['USA', 'Egypt', 'UK', 'Canada'])
        },
        "shippingCost": shipping_cost,
        "taxAmount": tax_amount,
        "taxRate": tax_rate,
        "discountPercentage": discount_percentage,
        "discountAmount": discount_amount,
        "finalAmount": final_amount,
        "orderStatus": random.choice(['pending', 'processing', 'shipped', 'delivered']),
        "shippingMethod": random.choice(['standard', 'express', 'overnight', 'pickup']),
        "discountCode": fake.lexify(text='????####') if discount_percentage > 0 else None,
        "isGift": random.choice([True, False]),
        "giftMessage": fake.sentence() if random.random() > 0.8 else None,
        "customerIpAddress": fake.ipv4(),
        "deviceType": random.choice(['mobile', 'desktop', 'tablet']),
        "browser": random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
        "returnEligible": random.choice([True, False]),
        "warrantyMonths": random.choice([0, 12, 24, 36])
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

    while (datetime.now() - curr_time).seconds < 86400 :
        try :    
            transaction = generate_sales_transactions()

            print(transaction)

            producer.produce(
                topic=topic,
                key=transaction['transactionId'],
                value=json.dumps(transaction),
                on_delivery = delivery_report
            )

            producer.poll(0)
            time.sleep(2)
        except BufferError as e:
            print(f"Local producer queue is full ({len(producer)} messages awaiting delivery): {e}")
       # finally :
          #  producer.flush()


if __name__ == "__main__":
    main()