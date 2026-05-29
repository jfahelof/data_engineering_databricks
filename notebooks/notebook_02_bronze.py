# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze
# MAGIC
# MAGIC  Neste notebook, iremos colocar os dados obtidos via API na camada Bronze. 

# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vamos criar um dataframe com spark para que possamos explorar os dados.

# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", "true") \
    .load("/Volumes/workspace/bronze/dbs/data.csv")

# COMMAND ----------

# Vamos visualizar os dados sem qualquer transformação 
df.limit(50).display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Vamos extrair ano, mês e dia da coluna 'data'
# antes disso, garantimos que a coluna está no tipo correto
df = df.withColumn("data", F.to_date("data"))

df = df.withColumn("ano", F.year("data")) \
       .withColumn("mes", F.month("data")) \
       .withColumn("dia", F.dayofmonth("data"))

# Adicionando a coluna 'data_carga_lake' com a data atual no formato yyyy-MM-dd
# Essa coluna registra quando os dados foram carregados no Data Lake,
# sendo essencial para auditoria, rastreabilidade e controle de versões dos dados.
df = df.withColumn("data_carga_lake", F.current_date())

# COMMAND ----------

df.limit(50).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Vamos salvar os dados brutos na camada Bronze
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN workspace;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.bronze;

# COMMAND ----------

# criar pasta para gravar o arquivo parquet

dbutils.fs.mkdirs("/Volumes/workspace/bronze/dbs/Sinistros_Transito")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gravação dos dados em formato Delta (Parquet)
# MAGIC
# MAGIC Nesta etapa, os dados são gravados utilizando o formato **Delta Lake**, que é construído sobre arquivos **Parquet**.
# MAGIC
# MAGIC O Parquet é um formato de armazenamento colunar, permitindo maior eficiência na compressão e melhor desempenho em consultas, pois apenas as colunas necessárias são lidas durante o processamento.
# MAGIC
# MAGIC O Delta Lake adiciona funcionalidades importantes ao Parquet, como controle de versão, transações ACID e suporte a cargas incrementais. Isso garante maior confiabilidade no processamento de dados, evitando inconsistências e permitindo operações como inserção, atualização e merge de dados.
# MAGIC
# MAGIC Além disso, será criada uma **tabela externa no Databricks SQL Warehouse**, apontando para os arquivos armazenados no Data Lake. Dessa forma, os dados podem ser consultados via SQL sem a necessidade de duplicação, facilitando a integração com ferramentas analíticas e de visualização.
# MAGIC
# MAGIC Com essa abordagem, obtemos um pipeline mais eficiente, escalável e confiável para o processamento de grandes volumes de dados.

# COMMAND ----------


# Grava os dados no formato Delta Lake no Data Lake, sobrescrevendo os existentes e permitindo evolução do schema
df.write.format("delta") \
  .mode("overwrite") \
  .option("mergeSchema","true") \
  .save("/Volumes/workspace/bronze/dbs/Sinistros_Transito/Sinistros_Transito_open_data")

# COMMAND ----------

# Cria o database bronze caso não exista
spark.sql("""CREATE DATABASE IF NOT EXISTS bronze;""")

# Registra a tabela Delta no catálogo a partir dos dados processados
df.write.format("delta") \
  .mode("overwrite") \
  .option("mergeSchema","true") \
  .saveAsTable("workspace.bronze.tb_Sinistros_Transito_open_data")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Para garantir que os dados foram inseridos na camada Bronze, vamos visualizar 

# COMMAND ----------

spark.sql("""
    SELECT * 
    FROM workspace.bronze.tb_Sinistros_Transito_open_data
    LIMIT 15
""").display()

# COMMAND ----------


