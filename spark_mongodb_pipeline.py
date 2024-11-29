from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round
from config import DATA_PATH, MONGO_URI, MONGO_DATABASE, MONGO_COLLECTION
from utils import validate_dataset

def create_spark_session():
    """criar sessão Spark com configuração para MongoDB."""
    return SparkSession.builder \
        .appName("Used Cars Pipeline") \
        .config("spark.mongodb.input.uri", f"{MONGO_URI}/{MONGO_DATABASE}.{MONGO_COLLECTION}") \
        .config("spark.mongodb.output.uri", f"{MONGO_URI}/{MONGO_DATABASE}.{MONGO_COLLECTION}") \
        .getOrCreate()

def process_data(spark, input_path):
    """executar o ETL no dataset."""
    # carregar dados do CSV
    df = spark.read.option("header", "true").csv(input_path)

    # validação do dataset
    if not validate_dataset(df):
        raise ValueError("dataset inválido verifique os dados.")

    # conversões e transformações
    df = df.withColumn("price", col("price").cast("float")) \
           .withColumn("mileage", col("mileage").cast("float")) \
           .filter((col("year") > 2015) & (col("price") >= 5000)) \
           .withColumn("price_per_km", round(col("price") / col("mileage"), 2))

    return df

def save_to_mongo(df):
    """salvar os dados transformados no MongoDB."""
    df.write \
      .format("mongo") \
      .mode("overwrite") \
      .save()

def main():
    # criar sessao Spark
    spark = create_spark_session()

    # processar os dados
    df_transformed = process_data(spark, DATA_PATH)

    # exibir amostra dos dados
    print("amostra dos dados transformados:")
    df_transformed.show()

    # salvar no MongoDB
    save_to_mongo(df_transformed)

    # finalizar a sessao Spark
    spark.stop()

if __name__ == "__main__":
    main()
