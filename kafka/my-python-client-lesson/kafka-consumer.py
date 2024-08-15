from kafka import KafkaConsumer
consumer = KafkaConsumer('quickstartevents',
bootstrap_servers=['localhost:9092'],
auto_offset_reset='earliest')
for msg in consumer:
    print (msg.value)