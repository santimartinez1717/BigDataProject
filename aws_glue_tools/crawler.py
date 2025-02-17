import boto3

def create_glue_database(database_name):
    client = boto3.client('glue')
    try:
        client.create_database(
            DatabaseInput={'Name': database_name}
        )
        print(f"Base de datos {database_name} creada exitosamente.")
    except client.exceptions.AlreadyExistsException:
        print(f"La base de datos {database_name} ya existe.")

def create_glue_crawler(crawler_name, database_name, s3_target_path, iam_role):
    client = boto3.client('glue')
    try:
        response = client.create_crawler(
            Name=crawler_name,
            Role=iam_role,
            DatabaseName=database_name,
            Targets={'S3Targets': [{'Path': s3_target_path}]},
            TablePrefix='trade_data_',
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            }
        )
        print(f"Crawler {crawler_name} creado exitosamente.")
    except client.exceptions.AlreadyExistsException:
        print(f"El crawler {crawler_name} ya existe.")

def start_glue_crawler(crawler_name):
    client = boto3.client('glue')
    response = client.start_crawler(Name=crawler_name)
    print(f"Crawler {crawler_name} iniciado correctamente.")

if __name__ == "__main__":
    grupo = "imat3b08"
    database_name = f"trade_data_{grupo}"
    crawler_name = f"trade_data_crawler_{grupo}"
    s3_target_path = "s3://historical-crypto-data21-25/"
    iam_role = "arn:aws:iam::911167885971:role/AWSGlueServiceRoleForCrypto"

    create_glue_database(database_name)
    create_glue_crawler(crawler_name, database_name, s3_target_path, iam_role)
    start_glue_crawler(crawler_name)
