import boto3


def create_glue_database(database_name):
    """Crea una base de datos en AWS Glue si no existe."""
    client = boto3.client("glue")
    try:
        client.create_database(DatabaseInput={"Name": database_name})
        print(f"✅ Base de datos {database_name} creada exitosamente.")
    except client.exceptions.AlreadyExistsException:
        print(f"ℹ️ La base de datos {database_name} ya existe.")


def create_glue_crawler(crawler_name, database_name, s3_target_path, iam_role):
    """Crea un solo crawler para todas las criptomonedas con particiones por symbol y year."""
    client = boto3.client("glue")
    try:
        client.create_crawler(
            Name=crawler_name,
            Role=iam_role,
            DatabaseName=database_name,
            Targets={"S3Targets": [{"Path": s3_target_path}]},
            TablePrefix="trade_data_",  # Prefijo de la tabla en Glue
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "DEPRECATE_IN_DATABASE",
            },
            Configuration='{"Version":1.0,"Grouping":{"TableLevelConfiguration":1}}',
        )
        print(f"✅ Crawler {crawler_name} creado correctamente.")
    except client.exceptions.AlreadyExistsException:
        print(f"ℹ️ El crawler {crawler_name} ya existe.")


def start_glue_crawler(crawler_name):
    """Inicia el crawler en AWS Glue."""
    client = boto3.client("glue")
    try:
        client.start_crawler(Name=crawler_name)
        print(f"🚀 Crawler {crawler_name} iniciado correctamente.")
    except client.exceptions.CrawlerRunningException:
        print(f"⚠️ El crawler {crawler_name} ya está en ejecución.")
    except client.exceptions.EntityNotFoundException:
        print(f"❌ El crawler {crawler_name} no existe.")


if __name__ == "__main__":
    grupo = "imat3b08"
    quality = "gold"
    database_name = f"trade_data_{grupo}_{quality}"
    iam_role = "arn:aws:iam::911167885971:role/AWSGlueServiceRoleForCrypto"
    bucket_name = f"crypto-historical-data-bucket-{quality}"

    # 🔹 Ruta del bucket raíz (incluye todas las criptos con subcarpetas como symbol=BTC/)
    s3_target_path = f"s3://{bucket_name}/"

    # 🔹 Nombre del crawler único para todo el bucket
    crawler_name = f"trade_data_crawler_{quality}"

    # Crear base de datos en Glue
    create_glue_database(database_name)

    # Crear y ejecutar un solo crawler
    create_glue_crawler(crawler_name, database_name, s3_target_path, iam_role)
    start_glue_crawler(crawler_name)
