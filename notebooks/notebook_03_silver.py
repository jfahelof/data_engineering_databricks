# Databricks notebook source
# MAGIC %md
# MAGIC # Silver
# MAGIC
# MAGIC Este notebook corresponde à **camada Silver** do pipeline de dados, sendo responsável pela padronização inicial e preparação dos dados provenientes da camada Bronze, disponíveis na tabela **`workspace.bronze.tb_Sinistros_Transito_open_data`** no Databricks.
# MAGIC
# MAGIC Nesta etapa, os dados são carregados e passam por um processo de **inferência e ajuste de tipos**. A conversão é realizada com foco em transformar colunas originalmente interpretadas como texto em tipos mais adequados, como inteiros, valores numéricos e datas, garantindo maior consistência e melhor desempenho durante o processamento analítico dos dados.
# MAGIC
# MAGIC Após essa etapa, os dados são convertidos novamente para um DataFrame Spark e persistidos no Data Lake no formato **Delta Lake (baseado em Parquet)**, garantindo eficiência de leitura, compressão e escalabilidade.
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação do spark Dataframe.

# COMMAND ----------

df_bronze = spark.read.table("workspace.bronze.tb_Sinistros_Transito_open_data")

# COMMAND ----------

df_bronze.limit(50).display()


# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remoção de duplicatas e dataframe silver
# MAGIC
# MAGIC

# COMMAND ----------

df_silver = df_bronze.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vamos remover as colunas vazias 

# COMMAND ----------

# MAGIC %md
# MAGIC ### A coluna com a informação de protocolo também será removida, pois não traz nenhuma informação útil para futuras análises. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Algumas colunas, faltam mais  de 50% das informações. Vamos removê-las. 

# COMMAND ----------

# vamos remover algumas colunas

colunas_para_remover = [
    'acidente_verificado', 'tempo_clima', 'situacao_semaforo', 'sinalizacao', 
    'condicao_via', 'conservacao_via', 'ponto_controle', 'situacao_placa', 
    'velocidade_max_via', 'mao_direcao', 'divisao_via1', 'divisao_via2', 
    'num_semaforo', 'sentido_via', 'Protocolo', 'detalhe_endereco_acidente', 'numero'
]

# Remove as colunas em lote
df_silver = df_silver.drop(*colunas_para_remover)

# COMMAND ----------

df_silver.limit(50).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Algumas colunas deveriam ser numéricas, mas estão como string. Vamos fazer a transformação para inteiros: 1,0 -> 1, 2,0 -> 2, ... isto será responsável pela redução de memória ocupada pelos dados, diminuindo gastos com cluster. 

# COMMAND ----------

colunas_numericas = [
    "auto", "moto", "ciclom", "ciclista", "pedestre",
    "onibus", "caminhao", "viatura", "outros",
    "vitimas", "vitimasfatais"]



#  convertendo para double antes de passar para int
df_silver = df_silver.select(
    *[
        F.coalesce(
            F.regexp_replace(F.col(c), ",", ".").cast("double").cast("int"), 
            F.lit(0)
        ).alias(c) if c in colunas_numericas else F.col(c)
        for c in df_silver.columns
    ]
)

# COMMAND ----------

df_silver.limit(50).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vamos pradronizar valores nulos

# COMMAND ----------

# Padroniza valores nulos nas colunas de texto
# Isso evita problemas em análises e garante consistência textual

colunas_texto = ["complemento", "bairro", "endereco", "bairro_cruzamento", "tipo"]


for col_txt in colunas_texto:
    df_silver = df_silver.withColumn(
        col_txt,
        F.coalesce(F.col(col_txt), F.lit("NA"))
    )


# COMMAND ----------

df_silver.limit(50).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Persistência dos dados na camada Silver
# MAGIC
# MAGIC #### Gravação dos dados em formato Delta Lake (Parquet)
# MAGIC
# MAGIC Nesta etapa, os dados tratados e transformados na camada Silver são persistidos no Data Lake utilizando o formato **Delta Lake**, que é construído sobre arquivos **Parquet**.
# MAGIC
# MAGIC O Parquet é um formato de armazenamento colunar que proporciona **alta compressão** e **leitura eficiente**, permitindo que apenas as colunas necessárias sejam processadas. Isso resulta em melhor desempenho e redução de custo computacional, especialmente em grandes volumes de dados.
# MAGIC
# MAGIC O Delta Lake complementa o Parquet ao adicionar funcionalidades essenciais para pipelines de dados modernos, como:
# MAGIC - controle de versão (time travel)  
# MAGIC - transações ACID (maior confiabilidade)  
# MAGIC - suporte a evolução de schema  
# MAGIC - base para cargas incrementais futuras  
# MAGIC
# MAGIC Embora o cenário atual não envolva ingestão incremental, a utilização do Delta Lake garante que o pipeline esteja preparado para evoluções futuras, mantendo escalabilidade e robustez.
# MAGIC
# MAGIC Após a gravação dos dados, será criada uma **tabela no catálogo do Databricks (Unity Catalog)**, permitindo consultas via SQL diretamente sobre os dados armazenados no Data Lake, sem necessidade de duplicação.
# MAGIC
# MAGIC Essa abordagem assegura um fluxo de dados eficiente, confiável e alinhado com boas práticas de engenharia de dados.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vamos criar o volume na camada Silver
# MAGIC
# MAGIC Quando rodei a primeira vez, tive que criar o volume. 

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE VOLUME workspace.silver.dbs;

# COMMAND ----------


# Grava os dados no formato Delta Lake na camada Silver (Data Lake),
# sobrescrevendo os dados e permitindo evolução do schema
df_silver.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save("/Volumes/workspace/silver/dbs/Sinistros_Transito/Sinistros_Transito_open_data")

# COMMAND ----------

# Vamos visualizar e validar dados ingeridos na camada Silver

spark.read.format("delta").load(
    "/Volumes/workspace/silver/dbs/Sinistros_Transito/Sinistros_Transito_open_data"
).createOrReplaceTempView("tb_Sinistros_Transito_open_data")

spark.sql("""SELECT * 
          FROM tb_Sinistros_Transito_open_data 
          LIMIT 50;"""
          ).display()
