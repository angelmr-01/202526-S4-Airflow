#!/usr/bin/env python3
import sys
import json

columnas_dataset = [
    "temperature_salon", "humidity_salon", "air_salon",
    "temperature_chambre", "humidity_chambre", "air_chambre",
    "temperature_bureau", "humidity_bureau", "air_bureau",
    "temperature_exterieur", "humidity_exterieur", "air_exterieur"
]

for linea in sys.stdin:
    linea = linea.strip()
    if not linea:
        continue

    try:
        datos = json.loads(linea)
        
        # 1. Validar que tiene clave primaria (la fecha en el dataset)
        timestamp = datos.get("timestamp")
        if not timestamp:
            continue  # Si no hay fecha, la fila no nos sirve
            
        valores_limpios = []
        fila_valida = True
        
        # 2. Validar que todos las columnas del dataset tienen datos y que son numéricos
        for col in columnas_dataset:
            valor = datos.get(col)
            
            if valor is None or valor == "":
                fila_valida = False
                break
                
            valores_limpios.append(str(float(valor)))
            
        # 3. Como el mapper y el reducer se comunican por la salida estándar
        #  (la consola), imprimimos los valores válidos
        if fila_valida:
            # Usamos el timestamp como Clave (para que Hadoop ordene por fecha)
            # y el resto de valores unidos como Valor
            print(f"{timestamp}\t{','.join(valores_limpios)}")
            
    except Exception:
        # Si el JSON está mal formado o un dato era texto en vez de número, se descarta
        continue