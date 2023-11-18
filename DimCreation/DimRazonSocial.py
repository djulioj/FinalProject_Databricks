# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("dimrazonsocial").getOrCreate()

# COMMAND ----------

# Read CSV
df = pd.read_csv(r'/Workspace/Repos/djulioj@uninorte.edu.co/FinalProject_Databricks/RegistroEmpresas.csv')

# Take all differente registers of the column 'RAZÓN SOCIAL' and create a dim table with them adding a new column 'id_razon_social'
dim_razon_social = df['RAZÓN SOCIAL'].drop_duplicates().reset_index(drop=True).reset_index()

# Rename the column 'index' to 'IdRazonSocial' and 'RAZÓN SOCIAL' to 'Nombre
dim_razon_social.rename(columns={'index':'IdRazonSocial', 'RAZÓN SOCIAL':'Nombre'}, inplace=True)

df_spark = spark.createDataFrame(dim_razon_social)

# Save the dim table
df_spark.write.format("delta").mode("overwrite").saveAsTable("default.dimrazonsocial")

# COMMAND ----------

# In the original table, add a new column 'id_razon_social' and fill it with the id of the dim table
df['id_razon_social'] = df['RAZÓN SOCIAL'].map(dim_razon_social.set_index('Nombre')['IdRazonSocial'])

# Delete the column 'RAZÓN SOCIAL' in the original table
df.drop(columns=['RAZÓN SOCIAL'], inplace=True)

# Save the original table with the new column in the original csv file
df.to_csv(r'RegistroEmpresas_StarModel.csv', index=False)
