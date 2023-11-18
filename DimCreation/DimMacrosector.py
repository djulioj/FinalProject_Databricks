# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("dimrazonsocial").getOrCreate()

# COMMAND ----------

# Read CSV
df = pd.read_csv(r'/Workspace/Repos/djulioj@uninorte.edu.co/FinalProject_Databricks/DimCreation/RegistroEmpresas_StarModel.csv')

# Take all differente registers of the column 'RAZÓN SOCIAL' and create a dim table with them adding a new column 'id_razon_social'
dim_macrosector = df['MACROSECTOR'].drop_duplicates().reset_index(drop=True).reset_index()

# Rename the column 'index' to 'IdSupervisor'
dim_macrosector.rename(columns={'index':'IdMacrosector', 'MACROSECTOR':'Nombre'}, inplace=True)

df_spark = spark.createDataFrame(dim_macrosector)

# Save the dim table
df_spark.write.format("delta").mode("overwrite").saveAsTable("default.dimmacrosector")

# COMMAND ----------

# In the original table, add a new column 'id_razon_social' and fill it with the id of the dim table. Column has no the same name in both tables
df['id_macrosector'] = df['MACROSECTOR'].map(dim_macrosector.set_index('Nombre')['IdMacrosector'])

# Delete the column 'RAZÓN SOCIAL' in the original table
df.drop(columns=['MACROSECTOR'], inplace=True)

# Save the original table with the new column in the original csv file
df.to_csv(r'RegistroEmpresas_StarModel.csv', index=False)
