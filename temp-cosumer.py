from confluent_kafka import Consumer, KafkaError

def consume_from_kafka(bootstrap_servers, topic_name):
    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'my-group',  # Consumer group ID
        'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the topic
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic_name])

    try:
        while True:
            msg = consumer.poll(1.0)  # Poll for messages

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            # Print received message value
            print('Received message: {}'.format(msg.value().decode('utf-8')))

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

# Example usage to consume and print messages from Kafka
if __name__ == '__main__':
    bootstrap_servers = 'localhost:9092'  # Replace with your Kafka broker address
    topic_name = 'projectck'  # Replace with your Kafka topic name

    consume_from_kafka(bootstrap_servers, topic_name)
