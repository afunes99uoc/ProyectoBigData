from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'clima-barcelona',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for mensaje in consumer:
    print("Mensaje recibido:", mensaje.value)
