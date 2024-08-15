import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

try:
    producer = KafkaProducer(bootstrap_servers=['kafka-2:9092'],value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    for n in range(3):
        producer.send('s1', {'c1': str(n + 7), 'c2' : n + 8})
except KafkaError as e:
    logging.error ("Error sending to kafka broker: {}", e)
    exit(1)
producer.flush()
producer.close()