# Práctica: Pipeline ETL con Airflow, Kafka, HDFS y PostgreSQL

Este repositorio contiene la infraestructura y el código (DAGs) necesarios para desplegar un pipeline de procesamiento de datos automatizado. El objetivo es ingerir, limpiar y analizar un dataset de monitorización de los sensores de una vivienda.

## Infraestructura y Tecnologías

- **Apache Kafka y Kafka Connect**: Ingesta de datos en streaming desde el origen.
- **Hadoop (HDFS)**: Almacenamiento distribuido (Data Lake) para persistir los archivos de datos en crudo y datos limpios.
- **Apache Airflow**: Orquestador central encargado de ejecutar las tareas del flujo de trabajo de manera secuencial.
- **PostgreSQL**: Base de datos (Data Warehouse) para almacenar los datos finales y permitir su interrogación.
- **Docker Compose**: Solución de contenerización para levantar todos los elementos y servicios bajo una red común.

## Instrucciones de Despliegue

1. Situarse en la raíz del repositorio.
2. Desplegar los servicios empaquetados abriendo la terminal y ejecutando el comando:
   ```bash
   docker-compose up -d --build
   ```
   *En el caso de utilizar Docker con WSL en Windows, es recomendable añadir un límite de recursos en el archivo `.wslconfig` interno de Windows para evitar sobrecargas de memoria.*

3. Una vez operativos los contenedores, acceder a la interfaz de Airflow en el navegador: `http://localhost:8081` (credenciales predeterminadas: `admin` / `admin`).

## Resumen del Flujo de Datos

El DAG `pipeline_end_to_end_sensores` ejecutará secuencialmente las siguientes fases:
1. Configuración del conector HDFS Sink en Kafka Connect.
2. Ingesta leyendo línea por línea el CSV original y publicándolos en Kafka.
3. Pausa de sincronización para que Kafka Connect complete la escritura física en disco.
4. Verificación de existencia de las carpetas `raw` en HDFS.
5. Descarga, limpieza exhaustiva y deduplicación temporal ejecutada directamente en Python. Todo ello se vuelca posteriormente compilado en un fichero limpio.
6. Preparación de esquema y tabla SQL en PostgreSQL.
7. Carga de los datos limpios en la base de datos relacional.
8. Ejecución automática de consultas analíticas y exportación a un reporte en texto plano.

## Resultados

Al término del DAG, en la ruta local correspondiente a `airflow/dags`, aparecerá un fichero denominado `reporte.txt` que compilará los resultados y lecturas extraídas de las métricas de la casa.
