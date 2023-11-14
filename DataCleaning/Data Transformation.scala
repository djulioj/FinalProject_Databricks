// Databricks notebook source
// MAGIC %sql
// MAGIC USE CATALOG hive_metastore;

// COMMAND ----------

// Librerias
import io.delta.tables._
import org.apache.spark.sql.functions._

// COMMAND ----------

spark.sql(
  s"""
    CREATE TABLE IF NOT EXISTS default.factregistroempresas (
      Nit BIGINT,
      IdRazonSocial BIGINT,
      IdSupervisor BIGINT,
      IdDepartamento BIGINT,
      IdCiudad BIGINT,
      Ciiu BIGINT,
      IdMacroSector BIGINT,
      IngresosOperacionales DECIMAL(13,2),
      Ganancia DECIMAL(13,2),
      TotalActivos DECIMAL(13,2),
      TotalPasivos DECIMAL(13,2),
      TotalPatrimonio DECIMAL(13,2),
      `AñoCorte` INT
    )
  """
)

// COMMAND ----------

val deltaTable = DeltaTable.forName("default.factregistroempresas")
val empresas = DeltaTable.forName("default.test").toDF
.withColumnRenamed("GANANCIA (PÉRDIDA)", "GANANCIA")

// COMMAND ----------

display(empresas)

// COMMAND ----------

println(empresas.columns.toList)

// COMMAND ----------

val gold =
empresas.alias("bronze")
.select(
  $"bronze.NIT".as("Nit"),
  $"bronze.id_razon_social".as("IdRazonSocial"),
  $"bronze.id_supervisor".as("IdSupervisor"),
  $"bronze.id_departamento".as("IdDepartamento"),
  $"bronze.id_ciudad".as("IdCiudad"),
  $"bronze.CIIU".as("Ciiu"),
  $"bronze.id_macrosector".as("IdMacroSector"),
  round(trim(translate($"bronze.INGRESOS OPERACIONALES", "$,", "")), 2).cast("DECIMAL(13,2)").as("IngresosOperacionales"),
  round(trim(translate($"bronze.GANANCIA", "$,", "")), 2).cast("DECIMAL(13,2)").as("Ganancia"),
  round(trim(translate($"bronze.TOTAL ACTIVOS", "$,", "")), 2).cast("DECIMAL(13,2)").as("TotalActivos"),
  round(trim(translate($"bronze.TOTAL PASIVOS", "$,", "")), 2).cast("DECIMAL(13,2)").as("TotalPasivos"),
  round(trim(translate($"bronze.TOTAL PATRIMONIO", "$,", "")), 2).cast("DECIMAL(13,2)").as("TotalPatrimonio"),
  $"bronze.Año de Corte".cast("INT").as("AñoCorte")
)

// COMMAND ----------

display(gold)

// COMMAND ----------

// Cargamos los datos en la tabla final
gold
.write
.format("delta")
.mode("overwrite")
.saveAsTable("default.factregistroempresas")
