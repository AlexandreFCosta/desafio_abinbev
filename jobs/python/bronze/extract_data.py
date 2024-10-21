from pyspark.sql import SparkSession
import requests
import json

spark = SparkSession.builder.appName("ExtractData").getOrCreate()

print("Extract Data")

def extract_data():
    url = 'https://api.openbrewerydb.org/breweries'
    response = requests.get(url)
    data = response.json()
    # Salvando na camada bronze dentro do volume mapeado
    bronze_path = '/opt/airflow/bronze_layer/breweries_raw.json'
    with open(bronze_path, 'w') as f:
        json.dump(data, f)


spark.stop()