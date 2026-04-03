import csv
import json
import logging
import requests
import tempfile
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from hdfs import InsecureClient
from datetime import datetime
from time import sleep
from kafka import KafkaProducer


# Lee un archivo CSV simulando sensores y envía cada fila a un tópico de Kafka.

def ingestar_csv_a_kafka(ruta_csv: str, topico: str):
    # Configurar el productor conectándose al puerto interno de Docker
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            # Serializamos el diccionario a JSON y luego a bytes (que es lo que entiende Kafka)
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        logging.info("Conectado a Kafka exitosamente.")
    except Exception as e:
        logging.error(f"Error al conectar con Kafka: {e}")
        raise

    # Leemos el CSV y lo enviamos línea a línea
    mensajes_enviados = 0
    with open(ruta_csv, mode='r', encoding='utf-8') as archivo:
        lector_csv = csv.DictReader(archivo)
        
        for fila in lector_csv:
            producer.send(topico, value=fila)
            mensajes_enviados += 1
            
            # Simulamos un pequeño retraso para no saturar la red de golpe
            sleep(0.01) 

    # Nos aseguramos de que todos los mensajes encolados se han enviado antes de cerrar
    producer.flush()
    producer.close()
    
    logging.info(f"Se han enviado {mensajes_enviados} mensajes al tópico '{topico}'.")



# Envía la configuración a la API REST de Kafka Connect para crear el HDFS Sink.

def configurar_kafka_connect():
    url_connect = "http://kafka-connect:8083/connectors"
    
    config_conector = {
        "name": "hdfs-sink-sensores",
        "config": {
            "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
            "tasks.max": "1",
            "topics": "sensor_data",
            "hdfs.url": "hdfs://namenode:9000",
            "topics.dir": "/data/raw",
            "format.class": "io.confluent.connect.hdfs.string.StringFormat",
            "flush.size": "100",
            "rotate.interval.ms": "60000",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.storage.StringConverter"
        }
    }

    try:
        # Hacemos la petición POST a Kafka Connect con la configuración del conector.
        respuesta = requests.post(url_connect, json=config_conector)
        
        # Si el conector ya existe (error 409), no pasa nada, lo ignoramos
        if respuesta.status_code == 409:
            logging.info("El conector HDFS ya estaba creado previamente.")
        else:
            respuesta.raise_for_status() # Lanza error si falla
            logging.info("Conector HDFS creado con éxito.")
            
    except Exception as e:
        logging.error(f"Error al configurar Kafka Connect: {e}")
        raise


# Procesa, limpia y transforma los datos antes de meterlos en la base de datos PostgreSQL

def procesar_datos():
    client = InsecureClient('http://namenode:9870', user='root')
    
    directorio_raw = '/data/raw/sensor_data'
    archivo_limpio = '/data/clean/sensor_data_clean.csv'
    
    columnas_dataset = [
        "temperature_salon", "humidity_salon", "air_salon",
        "temperature_chambre", "humidity_chambre", "air_chambre",
        "temperature_bureau", "humidity_bureau", "air_bureau",
        "temperature_exterieur", "humidity_exterieur", "air_exterieur"
    ]
    
    # Usamos un diccionario. La clave primaria será el timestamp. Esto elimina los
    # duplicados automáticamente (si llega el mismo timestamp, lo sobrescribe)
    datos_limpios = {} 
    
    print("Iniciando lectura y limpieza de datos...")
    
    # Recorremos todas las particiones y archivos que dejó Kafka Connect
    for particion in client.list(directorio_raw):
        ruta_particion = f"{directorio_raw}/{particion}"
        
        for archivo in client.list(ruta_particion):
            if archivo.endswith('.txt'):
                ruta_archivo = f"{ruta_particion}/{archivo}"
                print(f"Procesando archivo: {ruta_archivo}")
                
                # Leemos el archivo directamente desde HDFS
                with client.read(ruta_archivo, encoding='utf-8') as reader:
                    for linea in reader:
                        linea = linea.strip()
                        if not linea: continue

                        clave, valor = procesar_archivo(linea, columnas_dataset)

                        if clave and valor:
                            datos_limpios[clave] = valor

    # Ordenamos cronológicamente
    timestamps_ordenados = sorted(datos_limpios.keys())
    
    print(f"Se han procesado y limpiado {len(timestamps_ordenados)} registros únicos. Escribiendo a HDFS...")
    
    # 4. Escribimos el resultado final en un único archivo CSV en HDFS
    with client.write(archivo_limpio, encoding='utf-8', overwrite=True) as writer:
        for ts in timestamps_ordenados:
            writer.write(datos_limpios[ts])
            
    print("Transformación completada con éxito.")



def procesar_archivo(linea_cruda, columnas_dataset):
    try:
        datos = json.loads(linea_cruda)
        # Validamos que tiene clave primaria (campo fecha en el dataset)
        timestamp = datos.get("timestamp")
        
        if not timestamp: 
            return None, None
            
        valores_validos = []

        # Validamos que todos las columnas del dataset tienen datos y que son numéricos
        for col in columnas_dataset:
            valor = datos.get(col)
            if valor is None or valor == "":
                return None, None
                
            # Forzamos conversión a float para asegurar que es numérico
            valores_validos.append(str(float(valor)))
            
        linea_procesada = f"{timestamp},{','.join(valores_validos)}\n"
        
        return timestamp, linea_procesada
        
    except Exception:
        return None, None



# Definimos el esquema SQL de la tabla
sql_crear_tabla = """
CREATE TABLE IF NOT EXISTS sensor_data (
    timestamp TIMESTAMP PRIMARY KEY,
    temperature_salon NUMERIC,
    humidity_salon NUMERIC,
    air_salon NUMERIC,
    temperature_chambre NUMERIC,
    humidity_chambre NUMERIC,
    air_chambre NUMERIC,
    temperature_bureau NUMERIC,
    humidity_bureau NUMERIC,
    air_bureau NUMERIC,
    temperature_exterieur NUMERIC,
    humidity_exterieur NUMERIC,
    air_exterieur NUMERIC
);
"""


# Airflow hará de intermediario entre HDFS y Postgresql
def cargar_csv_a_postgres():
    hdfs_client = InsecureClient('http://namenode:9870', user='root')
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    ruta_hdfs = '/data/clean/sensor_data_clean.csv'
    
    print("Descargando CSV limpio desde HDFS...")
    
    # Creamos un archivo temporal en el contenedor de Airflow
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as tmp_file:
        with hdfs_client.read(ruta_hdfs, encoding='utf-8') as reader:
            for linea in reader:
                tmp_file.write(linea)
        ruta_local_tmp = tmp_file.name
        
    print("Inyectando datos en PostgreSQL mediante COPY...")
    
    try:
        # Limpiamos la tabla antes de cargar para evitar errores de clave duplicada 
        pg_hook.run("TRUNCATE TABLE sensor_data;")
        
        # Copiamos el archivo temporal de Airflow, que contiene todos los datos limpios,
        # a Postgresql
        pg_hook.copy_expert(
            sql="COPY sensor_data FROM STDIN WITH (FORMAT CSV, DELIMITER ',')",
            filename=ruta_local_tmp
        )
        print("Carga de datos completada.")
        
    finally:
        # Borramos el archivo temporal
        if os.path.exists(ruta_local_tmp):
            os.remove(ruta_local_tmp)



def registro_de_consultas():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    CONSULTAS = [
        {
            "titulo": "1. Estado general del piso (diferencia entre habitaciones)",
            "sql": """
                SELECT 
                    EXTRACT(MONTH FROM timestamp) AS mes,
                    ROUND(AVG(temperature_salon)::numeric, 2),
                    ROUND(AVG(temperature_bureau)::numeric, 2),
                    ROUND(AVG(humidity_salon)::numeric, 2),
                    ROUND(AVG(humidity_bureau)::numeric, 2),
                    ROUND(AVG(air_salon)::numeric, 0)
                FROM sensor_data
                GROUP BY EXTRACT(MONTH FROM timestamp)
                ORDER BY mes;
            """,
            "fetch_all": True,
            "limite_mostrar": 12,
            "plantilla_fila": "   - Mes {0:.0f} | Temp: Sal {1}°C vs Bur {2}°C | Hum: Sal {3}% vs Bur {4}% | CO2 Salón: {5}",
            "conclusion_final": "-> Conclusión: Muestra la situación base del piso. El Bureau es más cálido en verano, sugiriendo una carga térmica por los equipos informáticos. En noviembre la tendencia se invierte, lo que evidencia que la principal fuente de calor de la vivienda incide directamente sobre el Salón."
        },
        {
            "titulo": "2. Calidad del aire",
            "sql": """
                SELECT 
                    EXTRACT(MONTH FROM timestamp) AS mes,
                    ROUND(CORR(air_salon, humidity_salon)::numeric, 3)
                FROM sensor_data 
                GROUP BY EXTRACT(MONTH FROM timestamp)
                ORDER BY mes;
            """,
            "fetch_all": True,
            "limite_mostrar": 12,
            "plantilla_fila": "   - Mes {0:.0f}: Correlación Aire/Humedad de {1}.",
            "conclusion_final": "-> Conclusión: Indica si la casa ventila bien. En octubre la correlación sube, apuntando a que las ventanas se mantienen cerradas y el aire se estanca. En noviembre, el ambiente se reseca, un efecto típico provocado por la llegada del frío y el uso de climatización interior."
        },
        {
            "titulo": "3. Aislamiento térmico del piso",
            "sql": """
                SELECT 
                    EXTRACT(MONTH FROM timestamp) AS mes,
                    ROUND(AVG(temperature_salon - temperature_exterieur)::numeric, 2)
                FROM sensor_data 
                WHERE EXTRACT(HOUR FROM timestamp) BETWEEN 2 AND 6
                GROUP BY EXTRACT(MONTH FROM timestamp)
                ORDER BY mes;
            """,
            "fetch_all": True,
            "limite_mostrar": 12,
            "plantilla_fila": "   - Mes {0:.0f}: De madrugada, el interior se mantiene +{1}°C por encima del exterior.",
            "conclusion_final": "-> Conclusión: Mide la capacidad de los muros para retener temperatura. Retener tanto calor de noche dificulta el descanso en verano, pero es un indicador de buena eficiencia energética en invierno que permite ahorrar en climatización."
        },
        {
            "titulo": "4. EL LÍMITE DE TOLERANCIA AL CALOR (Punto de ruptura térmica)",
            "sql": """
                SELECT 
                    ROUND(temperature_exterieur::numeric, 0),
                    ROUND(AVG(temperature_salon)::numeric, 2)
                FROM sensor_data 
                GROUP BY ROUND(temperature_exterieur::numeric, 0)
                HAVING COUNT(*) > 3
                ORDER BY AVG(temperature_salon) DESC 
                LIMIT 1;
            """,
            "fetch_all": False,
            "plantilla_conclusion": "Cuando la calle registra {0}°C de forma recurrente, el interior de la vivienda alcanza su récord de calor con una media de {1}°C.\n-> Valor: Identifica el 'Punto de Ruptura' real de la casa, descartando espejismos estadísticos. Descubrir a qué temperatura exterior el piso se vuelve sofocante permite a una empresa anticiparse y lanzar promociones de refrigeración exactamente en la semana que el pronóstico del tiempo anuncie esos grados críticos."
        },
        {
            "titulo": "5. Efecto de los ordenadores en el bureau (pérdida de humedad)",
            "sql": """
                WITH ranking_termico AS (
                    SELECT 
                        (temperature_bureau - temperature_salon) AS diff_temp,
                        (humidity_bureau - humidity_salon) AS diff_hum,
                        PERCENT_RANK() OVER (ORDER BY temperature_bureau - temperature_salon) AS percentil
                    FROM sensor_data
                )
                SELECT 
                    ROUND(AVG(CASE WHEN percentil <= 0.20 THEN diff_hum END)::numeric, 2),
                    ROUND(AVG(CASE WHEN percentil >= 0.90 THEN diff_hum END)::numeric, 2)
                FROM ranking_termico;
            """,
            "fetch_all": False,
            "plantilla_conclusion": "Con los equipos en reposo, la diferencia de humedad es del {0}%. Sin embargo, en los momentos de mayor generación de calor, la humedad cae al {1}%.\n-> Valor: Demuestra matemáticamente que los pcs trabajando a alta carga resecan el cuarto, justificando la recomendación de usar humidificadores."
        }
    ]

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    with pg_hook.get_conn() as conexion:
        with conexion.cursor() as cursor:
            lineas_reporte = [
                "==============================================================",
                "      REPORTE GENERADO DE CONSULTAS A LA BASE DE DATOS        ",
                "      Generado el: " + datetime.now().strftime("%Y-%m-%d %H:%M"),
                "==============================================================\n"
            ]

            for item in CONSULTAS:
                cursor.execute(item["sql"])
                lineas_reporte.append(item["titulo"])
                
                if item.get("fetch_all"):
                    resultados = cursor.fetchall()
                    lineas_reporte.append("   Patrones temporales detectados:")
                    for fila in resultados[:item.get("limite_mostrar", 10)]:
                        lineas_reporte.append(item["plantilla_fila"].format(*fila))
                    lineas_reporte.append(item["conclusion_final"])
                
                else:
                    resultado = cursor.fetchone()
                    lineas_reporte.append(item["plantilla_conclusion"].format(*resultado))
                
                lineas_reporte.append("\n--------------------------------------------------------------\n")

    # Guardado del reporte final en el volumen compartido de Airflow
    ruta_reporte = '/opt/airflow/dags/reporte_ejecutivo_final.txt'
    with open(ruta_reporte, 'w', encoding='utf-8') as archivo:
        archivo.write("\n".join(lineas_reporte))




with DAG(
    dag_id='pipeline_end_to_end_sensores',
    description='Ingesta desde CSV a Kafka y volcado en HDFS',
    start_date=datetime(2026, 3, 30), # Fecha actual
    schedule_interval=None,
    catchup=False
) as dag:

    tarea_configurar_connect = PythonOperator(
        task_id='configurar_kafka_connect',
        python_callable=configurar_kafka_connect
    )

    tarea_producir_mensajes = PythonOperator(
        task_id='simular_sensores_kafka',
        python_callable=ingestar_csv_a_kafka,
        op_kwargs={
            'ruta_csv': '/opt/airflow/data/home_temperature_and_humidity_smoothed_filled.csv', 
            'topico': 'sensor_data'
        }
    )

    tarea_procesar_datos = PythonOperator(
        task_id='procesar_datos',
        python_callable=procesar_datos
    )

    tarea_crear_tabla = PostgresOperator(
        task_id='crear_tabla_postgres',
        postgres_conn_id='postgres_default',
        sql=sql_crear_tabla
    )

    tarea_cargar_postgres = PythonOperator(
        task_id='cargar_datos_postgres',
        python_callable=cargar_csv_a_postgres
    )


    tarea_registro_de_consultas = PythonOperator(
        task_id='registro_de_consultas',
        python_callable=registro_de_consultas
    )



    tarea_configurar_connect >> tarea_producir_mensajes >> tarea_procesar_datos >> tarea_crear_tabla >> tarea_cargar_postgres >> tarea_registro_de_consultas