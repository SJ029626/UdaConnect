from kafka import KafkaProducer
from flask import g


# Set up a Kafka producer
TOPIC_NAME = 'person'
KAFKA_SERVER = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)
# Setting Kafka to g enables us to use this
# in other parts of our application
g.kafka_producer = producer
