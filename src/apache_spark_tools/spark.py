from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BronzeToPlataProcessing") \
    .getOrCreate()

#Adaptar estos paths al local de cada uno
bronze_base_path = "/Users/mike/Documents/IMAT TERCERO 2.0/Tecnologias Big Data/BigDataProject/bronce"
plata_base_path = "/Users/mike/Documents/IMAT TERCERO 2.0/Tecnologias Big Data/BigDataProject/plata"

df = spark.read.option("header", "true").option("inferSchema", "true").csv(bronze_base_path)

df = df.withColumn("symbol", df["symbol"])

df.write.partitionBy("symbol", "year").parquet(plata_base_path, mode='overwrite')

print(f"Datos guardados en formato Parquet en: {plata_base_path}")