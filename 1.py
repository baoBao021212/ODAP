import csv
import time
import random
from confluent_kafka import Producer

# Function to read CSV file and send data to Kafka
def produce_from_csv_to_kafka(bootstrap_servers, topic_name, csv_file_path):
    # Kafka producer configuration
    producer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'csv-producer'
    }

    producer = Producer(producer_conf)

    # Open CSV file
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Convert row to a string message (you can customize this part based on your data structure)
            # Chỉ lấy value, key mình tự map 
            message_value = ','.join([f"{value}" for key, value in row.items()])

            # Send message to Kafka topic
            producer.produce(topic=topic_name, value=message_value.encode('utf-8'))

            # Random sleep between 1s to 3s
            time.sleep(random.uniform(1, 3))

    producer.flush()

# Example usage
if __name__ == '__main__':
    bootstrap_servers = 'localhost:9092'  # Replace with your Kafka broker address
    topic_name = 'projectck'  # Replace with your Kafka topic name
    csv_file_path = r".\credit_card\credit_card\credit_card_transactions-ibm_v2.csv"  # Replace with the path to your CSV file

    produce_from_csv_to_kafka(bootstrap_servers, topic_name, csv_file_path)
