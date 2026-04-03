#!/usr/bin/env python3
import sys

ultimo_timestamp = None

for linea in sys.stdin:
    linea = linea.strip()
    if not linea:
        continue

    try:
        # Separamos la clave (timestamp) del valor (los datos de los sensores)
        timestamp, resto_datos = linea.split('\t', 1)
        
        # Como Hadoop nos entrega los datos ordenados por la Clave (timestamp),
        # si vemos el mismo timestamp dos veces seguidas, lo ignoramos.
        if timestamp != ultimo_timestamp:
            
            # Reconstruimos la fila en formato CSV perfecto para PostgreSQL
            print(f"{timestamp},{resto_datos}")
            
            ultimo_timestamp = timestamp
            
    except ValueError:
        continue