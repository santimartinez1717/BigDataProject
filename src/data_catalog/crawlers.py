import boto3

def create_glue_database(database_name):
    """Crea una base de datos en AWS Glue si no existe."""
    client = boto3.client('glue')
    try:
        client.create_database(DatabaseInput={'Name': database_name})
        print(f"Base de datos {database_name} creada exitosamente.")
    except client.exceptions.AlreadyExistsException:
        print(f"La base de datos {database_name} ya existe.")

def create_glue_crawler(crawler_name, database_name, s3_target_path, iam_role):
    """Crea un crawler de AWS Glue."""
    client = boto3.client('glue')
    try:
        client.create_crawler(
            Name=crawler_name,
            Role=iam_role,
            DatabaseName=database_name,
            Targets={'S3Targets': [{'Path': s3_target_path}]},
            TablePrefix=f'trade_data_',
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            }
        )
        print(f"Crawler {crawler_name} creado exitosamente en {s3_target_path}.")
    except client.exceptions.AlreadyExistsException:
        print(f"El crawler {crawler_name} ya existe.")

def start_glue_crawler(crawler_name):
    """Inicia un crawler en AWS Glue."""
    client = boto3.client('glue')
    try:
        client.start_crawler(Name=crawler_name)
        print(f"Crawler {crawler_name} iniciado correctamente.")
    except client.exceptions.CrawlerRunningException:
        print(f"El crawler {crawler_name} ya está en ejecución.")
    except client.exceptions.EntityNotFoundException:
        print(f"El crawler {crawler_name} no existe.")

if __name__ == "__main__":
    grupo = "imat3b08"
    database_name = f"trade_data_{grupo}"
    iam_role = "arn:aws:iam::911167885971:role/AWSGlueServiceRoleForCrypto"
    bucket_name = "crypto-historical-data-bucket"

    # Lista de criptos con sus símbolos
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

    # Crear la base de datos en AWS Glue
    create_glue_database(database_name)

    # Crear y ejecutar un crawler por cada criptomoneda
    for crypto_name, symbol in cryptos.items():
        symbol_clean = symbol.removesuffix("USD")  # Quitar "USD" del símbolo
        s3_target_path = f"s3://{bucket_name}/{symbol_clean}/"
        crawler_name = f"trade_data_crawler_{symbol_clean.lower()}"

        create_glue_crawler(crawler_name, database_name, s3_target_path, iam_role)
        start_glue_crawler(crawler_name)
