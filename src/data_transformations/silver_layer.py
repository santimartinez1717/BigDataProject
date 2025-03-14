
import os
import boto3
from pyspark.sql import SparkSession


# Configuración de AWS y PySpark
AWS_REGION = "eu-south-2"
BUCKET_NAME_INPUT = "crypto-historical-data-bucket"  # Bucket de entrada (capa bronze)
BUCKET_NAME_OUTPUT = "crypto-silver-data-bucket"  # Bucket de salida (capa silver)

# Configuración de la sesión de Spark
spark = SparkSession.builder \
    .appName("Convert CSV to Parquet") \
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com") \
    .getOrCreate()


# Lista de criptomonedas
cryptos = {
    "Bitcoin": "BTCUSD",  
    "Ethereum": "ETHUSD", 
    "Ripple": "XRPUSD",  
    "Solana": "SOLUSD",  
    "Dogecoin": "DOGEUSD",
    "Cardano": "ADAUSD",  
    "Shiba Inu": "SHIBUSD", 
    "Polkadot": "DOTUSD",  
    "Aave": "AAVEUSD",  
    "Stellar": "XLMUSD", 
}

# Función para convertir CSV a Parquet y cargarlo a S3
def convert_csv_to_parquet():
    s3_client = boto3.client("s3")

    # Obtener la lista de archivos CSV desde el bucket de entrada
    for crypto_name, symbol in cryptos.items():
        # Definir el prefijo y la ruta del archivo CSV
        crypto_folder = symbol.removesuffix('USD')  # Sin "USD"
        file_prefix = f"{crypto_folder}/"

        # Listar objetos dentro del prefijo (carpeta del símbolo)
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME_INPUT, Prefix=file_prefix)

        if 'Contents' in response:
            for obj in response['Contents']:
                file_key = obj['Key']
                # Leer archivo CSV desde S3 con PySpark
                csv_file_path = f"s3a://{BUCKET_NAME_INPUT}/{file_key}"
                print(f"Convirtiendo {file_key} de CSV a Parquet...")

                # Leer el archivo CSV
                df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

                # Definir la ruta de salida para el archivo Parquet
                parquet_file_path = f"s3a://{BUCKET_NAME_OUTPUT}/{file_key.replace('.csv', '.parquet')}"

                # Guardar el DataFrame como Parquet
                df.write.parquet(parquet_file_path, mode="overwrite")
                print(f"Archivo convertido y guardado en: {parquet_file_path}")

        else:
            print(f"No se encontraron archivos para {crypto_name} en el bucket.")


if __name__ == "__main__":
    convert_csv_to_parquet()