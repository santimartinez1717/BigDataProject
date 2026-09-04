# BigDataProject — Pipeline de Datos de Criptomonedas en AWS

Pipeline de datos batch para la extracción, almacenamiento, catalogación y transformación de series históricas de precios de criptomonedas, desarrollado como proyecto de la asignatura de Tecnologías Big Data.

## Descripción

El proyecto implementa una arquitectura de datos por capas (**bronze → silver**) sobre AWS:

1. **Extracción (ETL):** descarga datos históricos diarios de 10 criptomonedas (Bitcoin, Ethereum, Ripple, Solana, Dogecoin, Cardano, Shiba Inu, Polkadot, Aave y Stellar) desde TradingView mediante WebSockets.
2. **Ingesta (bronze):** los datos se guardan en CSV y se suben a Amazon S3, particionados por `symbol` y `year`.
3. **Catalogación:** se crean automáticamente bases de datos y crawlers en AWS Glue que infieren el esquema de los datos en S3 y lo registran en el catálogo.
4. **Transformación (silver):** con Apache Spark (PySpark) los CSV se convierten a formato Parquet, manteniendo el particionado por `symbol` y `year`, para optimizar las consultas analíticas posteriores.

## Estructura del proyecto

```
BigDataProject/
├── src/
│   ├── etl/
│   │   ├── buckets.py              # Descarga de datos de TradingView y subida a S3 (capa bronze)
│   │   └── TradingviewData/        # Cliente para obtener históricos vía WebSocket de TradingView
│   ├── data_catalog/
│   │   ├── crawler.py              # Crawler único en Glue para todo el bucket (por símbolo/año)
│   │   └── crawlers.py             # Variante: un crawler de Glue por criptomoneda
│   ├── data_transformations/
│   │   ├── silver_layer.py         # Conversión CSV (S3) → Parquet (S3), capa silver
│   │   └── silver_layer.ipynb      # Notebook equivalente para procesamiento local con Spark
│   └── apache_spark_tools/
│       └── spark.py                # Utilidad Spark para pasar de bronze a plata en local
└── .gitignore
```

## Tecnologías

**AWS**
- **Amazon S3** — almacenamiento de datos en capas (bronze/silver), particionado por `symbol` y `year`
- **AWS Glue** — bases de datos, catálogo de datos y crawlers para inferencia automática de esquema
- **IAM** — roles y permisos para que Glue acceda a S3

**Otras**
- Python (boto3, pandas)
- Apache Spark / PySpark
- Formato Parquet
- API de TradingView (WebSockets)

## Requisitos

- Python 3.9+
- Credenciales de AWS configuradas (`aws configure` o variables de entorno) con permisos sobre S3, Glue e IAM
- Apache Spark instalado localmente para ejecutar `silver_layer.py` / `spark.py`

## Uso

1. **Descargar datos y subirlos a S3 (bronze):**
   ```bash
   python src/etl/buckets.py
   ```

2. **Crear la base de datos y el crawler en Glue:**
   ```bash
   python src/data_catalog/crawler.py
   ```

3. **Convertir de CSV a Parquet (silver):**
   ```bash
   python src/data_transformations/silver_layer.py
   ```

> Nota: las rutas de S3, la región (`eu-south-2`) y el rol de IAM están definidos como constantes al principio de cada script y deben adaptarse al entorno de cada usuario.
