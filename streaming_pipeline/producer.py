from confluent_kafka import Producer
from config import BOOTSTRAP_SERVERS,TOPIC
import json
# Kafka producer configuration
conf = {'bootstrap.servers': BOOTSTRAP_SERVERS}

# # Create a Kafka producer
# producer = Producer(conf)

# def produce_message(topic, key, value):
#     # Convert the value dictionary to a JSON string
#     json_value = json.dumps(value)
#     # Produce the message to the specified Kafka topic
#     producer.produce(topic=topic, key=str(key), value=json_value)
#     # Flush the producer to ensure the message is sent
#     producer.flush()


from confluent_kafka import Producer
import csv
import time

def read_csv(file_path):
    count = 0
    with open(file_path, 'r', encoding='utf-8-sig', errors='replace') as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row



def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to topic: {msg.topic()}, partition: {msg.partition()}, offset: {msg.offset()}')


def produce_data(bootstrap_servers, topic, file_path):
    producer = Producer({'bootstrap.servers': bootstrap_servers})

    try:
        for row in read_csv(file_path):
            # Convert the row to JSON string
            json_row = json.dumps(row)

            # Produce the message to the specified Kafka topic
            producer.produce(topic, value=json_row, callback=delivery_report)
            # Flush the producer to ensure the message is sent
            producer.flush()

    except KeyboardInterrupt:
        pass

    finally:
        producer.flush()


if __name__ == '__main__':
    bootstrap_servers = BOOTSTRAP_SERVERS  # Change this to your Kafka broker's address
    topic = TOPIC  # Change this to your Kafka topic name
    csv_file_path = './data/DataCoSupplyChainDataset.csv'  # Change this to the path of your CSV file

    produce_data(bootstrap_servers, topic, csv_file_path)
