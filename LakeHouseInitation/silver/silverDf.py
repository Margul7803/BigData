from pyspark.sql import SparkSession
import re

# Fonction pour nettoyer les noms de colonnes


def clean_column_names_auto(df):
    new_columns = [re.sub(r'[ ,;{}()\n\t=]', '_', col) for col in df.columns]
    return df.toDF(*new_columns)


# Chemins pour sauvegarder les fichiers dans la couche Bronze sous Iceberg
bronze_output_path_1 = "spark_catalog.db_name.bronze_alcohol_consumption_gdp"
bronze_output_path_2 = "spark_catalog.db_name.bronze_happiness_alcohol_consumption"
bronze_output_path_3 = "spark_catalog.db_name.bronze_alcohol_specific_deaths"

# Chemins pour sauvegarder les fichiers dans la couche Silver sous Iceberg
silver_output_path_1 = "spark_catalog.db_name.silver_alcohol_consumption_gdp"
silver_output_path_2 = "spark_catalog.db_name.silver_happiness_alcohol_consumption"
silver_output_path_3 = "spark_catalog.db_name.silver_alcohol_specific_deaths"

# Lire et transformer pour df_bronze_1
df_bronze_1 = spark.read.format("iceberg").load(bronze_output_path_1)

df_silver_1 = df_bronze_1 \
    .withColumnRenamed("Entity", "country_name") \
    .withColumnRenamed("Code", "country_code") \
    .withColumnRenamed("Year", "year") \
    .withColumnRenamed("Total_alcohol_consumption_per_capita__liters_of_pure_alcohol__projected_estimates__15+_years_of_age_", "total_alcohol_consumption_per_capita") \
    .withColumnRenamed("GDP_per_capita__PPP__constant_2017_international_$_", "gdp_per_capita_ppp") \
    .withColumnRenamed("Population__historical_estimates_", "population") \
    .withColumnRenamed("Continent", "continent")

# Filtrage des valeurs manquantes dans les colonnes essentielles
df_silver_1 = df_silver_1.filter(
    df_silver_1['total_alcohol_consumption_per_capita'].isNotNull() &
    df_silver_1['gdp_per_capita_ppp'].isNotNull() &
    df_silver_1['population'].isNotNull() &
    df_silver_1['country_name'].isNotNull()
)

# Supprimer les doublons éventuels
df_silver_1 = df_silver_1.dropDuplicates()

# Sauvegarde dans la couche Silver sous Iceberg avec partitionnement par année
df_silver_1.writeTo(silver_output_path_1).using(
    "iceberg").createOrReplace().partitionBy("year")

# Lire les données de la couche Bronze
df_bronze_2 = spark.read.format("iceberg").load(bronze_output_path_2)

# Transformation des colonnes et renommage
df_silver_2 = df_bronze_2 \
    .withColumnRenamed("Country", "country_name") \
    .withColumnRenamed("Region", "region_name") \
    .withColumnRenamed("Hemisphere", "hemisphere") \
    .withColumnRenamed("HappinessScore", "happiness_score") \
    .withColumnRenamed("HDI", "human_development_index") \
    .withColumnRenamed("GDP_PerCapita", "gdp_per_capita") \
    .withColumnRenamed("Beer_PerCapita", "beer_consumption_per_capita") \
    .withColumnRenamed("Spirit_PerCapita", "spirit_consumption_per_capita") \
    .withColumnRenamed("Wine_PerCapita", "wine_consumption_per_capita")

# Filtrer les données pour supprimer les lignes avec des valeurs manquantes
df_silver_2 = df_silver_2.filter(
    df_silver_2['happiness_score'].isNotNull() &
    df_silver_2['gdp_per_capita'].isNotNull() &
    df_silver_2['beer_consumption_per_capita'].isNotNull() &
    df_silver_2['spirit_consumption_per_capita'].isNotNull() &
    df_silver_2['wine_consumption_per_capita'].isNotNull() &
    df_silver_2['country_name'].isNotNull()
)

# Supprimer les doublons
df_silver_2 = df_silver_2.dropDuplicates()

# Sauvegarder les données transformées dans la couche Silver sous Iceberg
df_silver_2.writeTo(silver_output_path_2).using("iceberg").createOrReplace()

# Lire les données de la couche Bronze
df_bronze_3 = spark.read.format("iceberg").load(bronze_output_path_3)

# Transformation des colonnes et renommage
df_silver_3 = df_bronze_3 \
    .withColumnRenamed("Year_[note_3]", "year") \
    .withColumnRenamed("Sex", "sex") \
    .withColumnRenamed("ICD-10_code", "icd_10_code") \
    .withColumnRenamed("Individual_cause_of_death_description", "cause_of_death_description") \
    .withColumnRenamed("<1", "age_0_1") \
    .withColumnRenamed("01-04", "age_1_4") \
    .withColumnRenamed("05-09", "age_5_9") \
    .withColumnRenamed("10-14", "age_10_14") \
    .withColumnRenamed("15-19", "age_15_19") \
    .withColumnRenamed("20-24", "age_20_24") \
    .withColumnRenamed("25-29", "age_25_29") \
    .withColumnRenamed("30-34", "age_30_34") \
    .withColumnRenamed("35-39", "age_35_39") \
    .withColumnRenamed("40-44", "age_40_44") \
    .withColumnRenamed("45-49", "age_45_49") \
    .withColumnRenamed("50-54", "age_50_54") \
    .withColumnRenamed("55-59", "age_55_59") \
    .withColumnRenamed("60-64", "age_60_64") \
    .withColumnRenamed("65-69", "age_65_69") \
    .withColumnRenamed("70-74", "age_70_74") \
    .withColumnRenamed("75-79", "age_75_79") \
    .withColumnRenamed("80-84", "age_80_84") \
    .withColumnRenamed("85-89", "age_85_89") \
    .withColumnRenamed("90+", "age_90_plus") \
    .withColumnRenamed("All_ages", "all_ages")

# Gérer les valeurs manquantes : filtrer les lignes avec des valeurs nulles dans les colonnes essentielles
df_silver_3 = df_silver_3.filter(
    df_silver_3['year'].isNotNull() &
    df_silver_3['cause_of_death_description'].isNotNull()
)

# Supprimer les doublons
df_silver_3 = df_silver_3.dropDuplicates()

# Sauvegarder les données transformées dans la couche Silver sous Iceberg
df_silver_3.writeTo(silver_output_path_3).using("iceberg").createOrReplace()
