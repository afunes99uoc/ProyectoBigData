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

while True:
    for _, row in df.iterrows():
        try:
            url = (
                f"https://api.open-meteo.com/v1/forecast?"
                f"latitude={row.lat}&longitude={row.lon}"
                f"&current=temperature_2m,wind_speed_10m,relative_humidity_2m,precipitation,surface_pressure"
            )
            response = requests.get(url)
            current = response.json().get("current", {})

            mensaje = {
                "ciudad": row.nom_barri,
                "temperatura_2m": current.get("temperature_2m"),
                "wind_speed_10m": current.get("wind_speed_10m"),
                "relative_humidity_2m": current.get("relative_humidity_2m"),
                "precipitation": current.get("precipitation"),
                "surface_pressure": current.get("surface_pressure")
            }

            print("Enviando:", mensaje)
            producer.send("clima-barcelona", mensaje)
        except Exception as e:
            print(f"Error con {row.nom_barri}: {e}")
    time.sleep(10)
