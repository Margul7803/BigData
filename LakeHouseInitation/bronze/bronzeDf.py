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

# Chemins pour sauvegarder les fichiers dans la couche Bronze sous Iceberg
# Utilisation d'un catalogue Iceberg et d'un entrepôt Hadoop
bronze_output_path_1 = "spark_catalog.db_name.bronze_alcohol_consumption_gdp"
bronze_output_path_2 = "spark_catalog.db_name.bronze_happiness_alcohol_consumption"
bronze_output_path_3 = "spark_catalog.db_name.bronze_alcohol_specific_deaths"

# Lecture des fichiers CSV bruts dans Spark
df_bronze_1 = spark.read.csv(
    bronze_input_path_1, header=True, inferSchema=True)
df_bronze_2 = spark.read.csv(
    bronze_input_path_2, header=True, inferSchema=True)

# Lecture du fichier Excel brut avec Spark
df_bronze_3 = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(bronze_input_path_3)

# Nettoyage des noms de colonnes
df_bronze_1_clean = clean_column_names_auto(df_bronze_1)
df_bronze_2_clean = clean_column_names_auto(df_bronze_2)
df_bronze_3_clean = clean_column_names_auto(df_bronze_3)

# Sauvegarde des DataFrames dans la couche Bronze sous Iceberg
df_bronze_1_clean.writeTo(bronze_output_path_1).using(
    "iceberg").createOrReplace()
df_bronze_2_clean.writeTo(bronze_output_path_2).using(
    "iceberg").createOrReplace()
df_bronze_3_clean.writeTo(bronze_output_path_3).using(
    "iceberg").createOrReplace()

# Affichage des tables Iceberg dans le catalogue
spark.sql("SHOW TABLES IN spark_catalog.db_name").show()

print("Données sauvegardées dans la couche Bronze sous Iceberg.")
