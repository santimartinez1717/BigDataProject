import os
import boto3
from TradingviewData import TradingViewData, Interval

# Configuración de AWS
AWS_REGION = "eu-south-2"  # Cambia esto si es necesario
BUCKET_NAME = "crypto-historical-data-bucket"  # Nombre del bucket

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

def create_s3_bucket(s3_client, bucket_name):
    """Crea un bucket en S3 si no existe."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"El bucket '{bucket_name}' ya existe.")
    except:
        print(f"Creando el bucket '{bucket_name}'...")
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': AWS_REGION})
        print(f"Bucket '{bucket_name}' creado exitosamente.")

def upload_to_s3(s3_client, bucket_name, file_path, object_name):
    """Sube un archivo a S3."""
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"Subido: {object_name}")
    except Exception as e:
        print(f"Error subiendo {object_name}: {e}")

def download_data_for_cryptos():
    s3_client = boto3.client("s3")
    create_s3_bucket(s3_client, BUCKET_NAME)

    tv = TradingViewData()
    for crypto_name, symbol in cryptos.items():
        print(f"Obteniendo datos para {crypto_name}...")
        data = tv.get_hist(symbol, exchange="COINBASE", interval=Interval.daily, n_bars=4*365)

        lenght_new_datasets = 365
        lenght_dataset = len(data)
        data_years = [(data[i:i+lenght_new_datasets], data.index[i].year) for i in range(0, lenght_dataset, lenght_new_datasets)]

        for data_year in data_years:
            year = data_year[1]
            local_file = f"{crypto_name}_historical_data_{year}.csv"
            object_name = f"symbol={cryptos[crypto_name].removesuffix('USD') }/year={year}/{local_file}"
            
            data_year[0].to_csv(local_file)
            upload_to_s3(s3_client, BUCKET_NAME, local_file, object_name)
            os.remove(local_file)  # Eliminar archivo local después de subirlo

        print(f"Datos de {crypto_name} guardados exitosamente en S3.")

if __name__ == "__main__":
    download_data_for_cryptos()
