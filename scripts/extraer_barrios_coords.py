import pandas as pd
import re

def extraer_lat_lon(polygon):
    match = re.search(r'\(\(([^)]+)\)\)', polygon)
    if match:
        puntos = match.group(1).split(", ")
        lon, lat = map(float, puntos[0].split())
        return pd.Series({'lat': lat, 'lon': lon})
    return pd.Series({'lat': None, 'lon': None})

df_barrios = pd.read_csv("data/Bronce/BarcelonaCiutat_Barris.csv")
coordenadas = df_barrios['geometria_wgs84'].apply(extraer_lat_lon)
df_coords = pd.concat([df_barrios[['nom_barri']], coordenadas], axis=1)
df_coords.to_csv("data/Plata/barrios_coordenadas.csv", index=False)
print("Archivo barrios_coordenadas.csv generado correctamente.")
