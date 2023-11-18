# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("dimrazonsocial").getOrCreate()

# COMMAND ----------

# Read CSV
df = pd.read_csv(r'/Workspace/Repos/djulioj@uninorte.edu.co/FinalProject_Databricks/DimCreation/RegistroEmpresas_StarModel.csv')

# Take all differente registers of the column 'SUPERVISOR' and create a dim table with them adding a new column 'id_razon_social'
dim_supervisor = df['SUPERVISOR'].drop_duplicates().reset_index(drop=True).reset_index()

# Rename the column 'index' to 'IdSupervisor' and 'SUPERVISOR' to 'Nombre
dim_supervisor.rename(columns={'index':'IdSupervisor', 'SUPERVISOR':'Nombre'}, inplace=True)

df_spark = spark.createDataFrame(dim_supervisor)

# Save the dim table
df_spark.write.format("delta").mode("overwrite").saveAsTable("default.dimsupervisor")

# COMMAND ----------

# In the original table, add a new column 'id_supervisor' and fill it with the id of the dim table
df['id_supervisor'] = df['SUPERVISOR'].map(dim_supervisor.set_index('Nombre')['IdSupervisor'])

# Delete the column 'SUPERVISOR' in the original table
df.drop(columns=['SUPERVISOR'], inplace=True)

# Save the original table with the new column in the original csv file
df.to_csv(r'RegistroEmpresas_StarModel.csv', index=False)
