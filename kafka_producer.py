from kafka import KafkaProducer
import random
import time

from datetime import datetime, timedelta

# Sample data to choose from
product_name = ["Television", "Mobile", "Oven","Desktop","Laptop"]

start_date = datetime(2020, 1, 1)
end_date = datetime(2025, 1, 1)

random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
random_date=random_date.strftime('%Y-%m-%d')

# Create a KafkaProducer
producer = KafkaProducer(bootstrap_servers=['kafka:9093'])

# Function to generate a random healthcare record
def generate_random_data():
    transaction_id = random.randint(1, 1000)  
    quantity = random.randint(1, 10)  
    category = random.choice(["Electronics", "Accessories"])  
    product_name = random.choice(product_name) 
    price = round(random.uniform(500, 5000), 2)  
    transaction_date = random_date 
    
    # Create a dictionary with the generated values
    data = {
        "transaction_id": transaction_id,
        "quantity": quantity,
        "category": category,
        "product_name": product_name,
        "price": price,
        "transaction_date": transaction_date
    }
    
    return data

# Function to continuously stream data
def stream_data():
    while True:
        # Generate random healthcare data
        data = generate_random_data()
        
        # Send the data to the Kafka topic
        producer.send('retail_topic', value=str(data).encode('utf-8'))
        
        # Print the generated data (for debugging or logging)
        print(data)
        
        # Wait for 1 second before sending the next record
        time.sleep(1)

if __name__ == "__main__":
    stream_data()

# from kafka import KafkaProducer
# import csv

# producer = KafkaProducer(bootstrap_servers=['kafka:9093'])

# def stream_data():
#     with open('healthcare_data.csv', 'r') as file:
#         reader = csv.DictReader(file)
#         for row in reader:
#             producer.send('healthcare_topic', value=str(row).encode('utf-8'))

# if __name__ == "__main__":
#     stream_data()