from pyspark.sql import functions as F

# Table de faits : Consommation d'alcool et PIB
fact_alcohol_consumption_gdp = df_silver_1 \
    .join(df_silver_2.select("country_name", "gdp_per_capita"), "country_name", "left") \
    .withColumn("year", F.col("year").cast("int")) \
    .select(
        "country_code",
        "year",
        "total_alcohol_consumption_per_capita",
        "gdp_per_capita_ppp",
        "population"
    )
fact_alcohol_consumption_gdp.printSchema()

# Table de faits : Bonheur et consommation d'alcool
fact_happiness_alcohol_consumption = df_silver_2 \
    .withColumn("year", F.lit(2018)) \
    .select(
        "country_name",
        "year",
        "happiness_score",
        "beer_consumption_per_capita",
        "spirit_consumption_per_capita",
        "wine_consumption_per_capita"
    ) \
    .join(df_silver_1.select("country_name", "country_code"), "country_name", "left") \
    .select(
        "country_code",
        "year",
        "happiness_score",
        "beer_consumption_per_capita",
        "spirit_consumption_per_capita",
        "wine_consumption_per_capita"
    )

# Table de faits : Décès spécifiques liés à l'alcool
fact_alcohol_specific_deaths = df_silver_3 \
    .select(
        "year",
        "sex",
        "cause_of_death_description",
        "age_0_1", "age_1_4", "age_5_9", "age_10_14", "age_15_19",
        "age_20_24", "age_25_29", "age_30_34", "age_35_39", "age_40_44",
        "age_45_49", "age_50_54", "age_55_59", "age_60_64", "age_65_69",
        "age_70_74", "age_75_79", "age_80_84", "age_85_89", "age_90_plus", "all_ages"
    )
fact_alcohol_specific_deaths.printSchema()

# Table dimensionnelle : Pays
dim_country = df_silver_1.select(
    "country_code",
    "country_name",
    "continent"
).distinct()
dim_country.printSchema()

# Table dimensionnelle : Année
dim_year = df_silver_1.select("year").distinct()
dim_year.printSchema()

# Table dimensionnelle : Cause de décès
dim_death_cause = df_silver_3.select(
    "cause_of_death_description"
).distinct()
dim_death_cause.printSchema()

# Chemins pour sauvegarder les données dans le stockage Gold sous Iceberg
fact_alcohol_consumption_gdp_path = "spark_catalog.db_name.gold_fact_alcohol_consumption_gdp"
fact_happiness_alcohol_consumption_path = "spark_catalog.db_name.gold_fact_happiness_alcohol_consumption"
fact_alcohol_specific_deaths_path = "spark_catalog.db_name.gold_fact_alcohol_specific_deaths"
dim_country_path = "spark_catalog.db_name.gold_dim_country"
dim_year_path = "spark_catalog.db_name.gold_dim_year"
dim_death_cause_path = "spark_catalog.db_name.gold_dim_death_cause"

# Sauvegarde des tables dans le stockage Gold au format Iceberg
fact_alcohol_consumption_gdp.writeTo(
    fact_alcohol_consumption_gdp_path).using("iceberg").createOrReplace()
fact_happiness_alcohol_consumption.writeTo(
    fact_happiness_alcohol_consumption_path).using("iceberg").createOrReplace()
fact_alcohol_specific_deaths.writeTo(
    fact_alcohol_specific_deaths_path).using("iceberg").createOrReplace()
dim_country.writeTo(dim_country_path).using("iceberg").createOrReplace()
dim_year.writeTo(dim_year_path).using("iceberg").createOrReplace()
dim_death_cause.writeTo(dim_death_cause_path).using(
    "iceberg").createOrReplace()
