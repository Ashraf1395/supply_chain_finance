from confluent_kafka import Consumer, KafkaError

# Kafka consumer configuration
conf = {'bootstrap.servers': "localhost:9092", 'group.id': "ashraf.de-zoomcamp-form-data", 'auto.offset.reset': 'earliest'}
consumer = Consumer(conf)

# # Subscribe to the Kafka topic
# topic = 'ashraf-de-form-submissions'
# consumer.subscribe([topic])

# try:
#     while True:
#         # Poll for new messages
#         msg = consumer.poll(timeout=1.0)
#         if msg is None:
#             continue
#         if msg.error():
#             if msg.error().code() == KafkaError._PARTITION_EOF:
#                 # End of partition, ignore
#                 continue
#             else:
#                 # Error occurred
#                 print(msg.error())
#                 break
#         # Process the message
#         print('Received message: {}'.format(msg.value().decode('utf-8')))
# except KeyboardInterrupt:
#     pass
# finally:
#     # Close the consumer
#     consumer.close()



from confluent_kafka import Consumer, KafkaError
import json
from config import BOOTSTRAP_SERVERS,TOPIC
def consume_data(bootstrap_servers, topic):
    conf = {'bootstrap.servers': bootstrap_servers, 'group.id': 'ashraf-supply-chain', 'auto.offset.reset': 'earliest'}
    consumer = Consumer(conf)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    continue
                else:
                    print(msg.error())
                    break
            else:
                # Message value and key are raw bytes -- decode if necessary!
                # e.g., convert to utf-8
                print('Received message: {}'.format(msg.value().decode('utf-8')))

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()

if __name__ == '__main__':
    bootstrap_servers = BOOTSTRAP_SERVERS  # Change this to your Kafka broker's address
    topic = TOPIC  # Change this to your Kafka topic name

    consume_data(bootstrap_servers, topic)
