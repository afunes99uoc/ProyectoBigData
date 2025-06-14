from kafka import KafkaProducer
import json
import time
import requests

# Configuración del productor
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# URL de Open-Meteo (Barcelona)
url = 'https://api.open-meteo.com/v1/forecast?latitude=41.38&longitude=2.17&hourly=temperature_2m,relative_humidity_2m'

# Simulación de datos cada 5 segundos
while True:
    response = requests.get(url)
    data = response.json()
    horas = data["hourly"]["time"]
    temperaturas = data["hourly"]["temperature_2m"]
    humedades = data["hourly"]["relative_humidity_2m"]

    for i in range(len(horas)):
        mensaje = {
            "hora": horas[i],
            "temperatura": temperaturas[i],
            "humedad": humedades[i]
        }
        print("Enviando:", mensaje)
        producer.send('clima-barcelona', value=mensaje)
        time.sleep(1)

