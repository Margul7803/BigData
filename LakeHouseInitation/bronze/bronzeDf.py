from pyspark.sql import SparkSession
import re

# Function to clean column names


def clean_column_names_auto(df):
    new_columns = [re.sub(r'[ ,;{}()\n\t=]', '_', col) for col in df.columns]
    return df.toDF(*new_columns)


# Chemins des fichiers bruts dans le container Azure
bronze_input_path_1 = "/mnt/conteneurmarioabjmb/alcohol-consumption-vs-gdp-per-capita.csv"
bronze_input_path_2 = "/mnt/conteneurmarioabjmb/HappinessAlcoholConsumption.csv"
bronze_input_path_3 = "/mnt/conteneurmarioabjmb/Alcohol-specific deaths in the UK.xlsx"

# Chemins pour sauvegarder les fichiers dans la couche Bronze
bronze_output_path_1 = "/mnt/conteneurmarioabjmb/bronze/alcohol_consumption_gdp"
bronze_output_path_2 = "/mnt/conteneurmarioabjmb/bronze/happiness_alcohol_consumption"
bronze_output_path_3 = "/mnt/conteneurmarioabjmb/bronze/alcohol_specific_deaths"

# Lecture des fichiers CSV bruts dans Spark
df_bronze_1 = spark.read.csv(
    bronze_input_path_1, header=True, inferSchema=True)
df_bronze_2 = spark.read.csv(
    bronze_input_path_2, header=True, inferSchema=True)

# Lecture du fichier Excel brut avec Pandas, puis conversion en DataFrame Spark
df_bronze_3 = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(bronze_input_path_3)

df_bronze_1_clean = clean_column_names_auto(df_bronze_1)
df_bronze_2_clean = clean_column_names_auto(df_bronze_2)
df_bronze_3_clean = clean_column_names_auto(df_bronze_3)

# Sauvegarde des DataFrames dans la couche Bronze (en format Parquet ou Delta)
df_bronze_1_clean.write.format("delta").mode(
    "overwrite").save(bronze_output_path_1)
df_bronze_2_clean.write.format("delta").mode(
    "overwrite").save(bronze_output_path_2)
df_bronze_3_clean.write.format("delta").mode(
    "overwrite").save(bronze_output_path_3)

print("Données sauvegardées dans la couche Bronze.")
