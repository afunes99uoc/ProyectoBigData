# simular_kafka_clima.py
from kafka import KafkaProducer
import requests
import json
import time
import pandas as pd

# Cargar barrios con coordenadas
df = pd.read_csv("data/plata/barrios_coordenadas.csv")

# Configurar productor Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Bucle infinito para emitir datos
while True:
    for _, row in df.iterrows():
        try:
            response = requests.get(
                f"https://api.open-meteo.com/v1/forecast?latitude={row.lat}&longitude={row.lon}&current_weather=true"
            )
            data = response.json().get("current_weather", {})
            mensaje = {
                "ciudad": row.nom_barri,
                "temperatura": data.get("temperature"),
                "humedad": 60  # puedes enriquecer esto si obtienes más datos reales
            }
            print("Enviando:", mensaje)
            producer.send("clima-barcelona", mensaje)
        except Exception as e:
            print(f"Error con {row.nom_barri}: {e}")
    time.sleep(5)
