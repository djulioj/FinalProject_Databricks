# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("dimrazonsocial").getOrCreate()

# COMMAND ----------

# Read CSV
df = pd.read_csv(r'/Workspace/Repos/djulioj@uninorte.edu.co/FinalProject_Databricks/DimCreation/RegistroEmpresas_StarModel.csv')

dim_departamento = spark.table("default.dimdepartamento")

# Divide the Column Ciudad Domicilio in two columns: 'Ciudad' and 'id_departamento'
df1 = df['CIUDAD DOMICILIO'].str.split('-', expand=True)
df1.rename(columns={0:'Nombre', 1:'Departamento'}, inplace=True)

# Take just columns 'Ciudad' and 'Departamento'
df1 = df1[['Nombre', 'Departamento']]

df1['id_departamento'] = df1['Departamento'].map(dim_departamento.set_index('Nombre')['IdDepartamento'])

# Replace NaN values with -1
df1['id_departamento'].fillna(-1, inplace=True)

# Convert the column 'id_departamento' to int
df1['id_departamento'] = df1['id_departamento'].astype(int)

df1 = df1[['Nombre', 'id_departamento']]

# Drop duplicates from df1 based on 'Nombre' column
df1 = df1.drop_duplicates(subset=['Nombre'])

# Reset the index
df1 = df1.reset_index(drop=True)

# Rename the index column to 'IdCiudad'
df1.rename(columns={'index': 'IdCiudad'}, inplace=True)

# Create a new column 'IdCiudad'
df1['IdCiudad'] = df1.index

df_spark = spark.createDataFrame(df1)

# Save the dim table
df_spark.write.format("delta").mode("overwrite").saveAsTable("default.dimciudad")

# COMMAND ----------

# In the original table, add a new column 'id_ciudad' and fill it with the id of the dim table
df['id_ciudad'] = df['CIUDAD DOMICILIO'].str.split('-', expand=True)[0]

# Delete the column 'CIUDAD DOMICILIO' in the original table
df.drop(columns=['CIUDAD DOMICILIO'], inplace=True)

# Save the original table with the new column in the original csv file
df.to_csv(r'RegistroEmpresas_StarModel2.csv', index=False)
