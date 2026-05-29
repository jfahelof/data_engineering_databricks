# Databricks notebook source
# MAGIC %md
# MAGIC # Gold
# MAGIC
# MAGIC Este notebook corresponde à **camada Gold** do pipeline de dados, sendo responsável pela padronização dos dados provenientes da camada Silver, disponíveis na tabela **`workspace.silver.tb_Sinistros_Transito_open_data`** no Databricks.
# MAGIC
# MAGIC A camada Gold da arquitetura medallion é a etapa da pipeline de dados responsável pela disponibilização de dados refinados, agregados e prontos para consumo analítico. Nessa camada, são aplicadas regras de negócio, métricas e transformações que permitem gerar insights estratégicos, dashboards e análises preditivas.
# MAGIC
# MAGIC Como o projeto utiliza dados de sinistros de trânsito da cidade do Recife, o foco da camada Gold foi a construção de tabelas analíticas capazes de responder questões relevantes para a mobilidade urbana e segurança no trânsito. Entre os exemplos de análises desenvolvidas estão: bairros com maior número de acidentes, horários de maior ocorrência, tipos de veículos mais envolvidos em sinistros e meios de transporte associados a acidentes fatais.
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação do spark Dataframe

# COMMAND ----------

df_silver = spark.read.format("delta").load(
    "/Volumes/workspace/silver/dbs/Sinistros_Transito/Sinistros_Transito_open_data"
)

# COMMAND ----------

df_silver.limit(30).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informações por bairro
# MAGIC
# MAGIC ### Queremos saber informações apuradas para cada bairro. Perguntas como: "Quais são os bairros com maior número de acidentes?", "Qual bairro tem acidentes mais fatais?" e várias outras podem ser respondidas a partir dessa tabela.
# MAGIC
# MAGIC ###Nela, cada linha representa um bairro, contendo o total de acidentes registrados, o número total de vítimas, o número de vítimas fatais e a taxa de fatalidade (razão entre vítimas fatais e vítimas totais).
# MAGIC
# MAGIC ###Essa estrutura permite identificar regiões com maior concentração de ocorrências, avaliar a gravidade média dos acidentes e comparar o risco relativo entre diferentes bairros, servindo como base para análises mais profundas e tomada de decisão orientada por dados.

# COMMAND ----------

df_bairro = (
    df_silver
    .groupBy("bairro")
    .agg(
        F.count("bairro").alias("total_acidentes"),
        F.sum("vitimas").alias("total_vitimas"),
        F.sum("vitimasfatais").alias("total_fatais")
    )
    # Adiciona coluna formatada para o Power BI reconhecer geograficamente
    .withColumn("localizacao", F.concat(F.col("bairro"), F.lit(", Recife, Pernambuco, Brasil")))
    # Calcula taxa de fatalidade evitando divisão por zero com NULLIF
    .withColumn("taxa_fatalidade", F.col("total_fatais") / F.nullif(F.col("total_vitimas"), F.lit(0)))
    # CORREÇÃO AQUI: Usando Window.partitionBy() vazio para pegar o total geral da tabela
    .withColumn("percentual_acidentes", (F.col("total_acidentes") / F.sum("total_acidentes").over(Window.partitionBy())) * 100)
    # Ordena de forma decrescente por acidentes
    .orderBy(F.col("total_acidentes").desc())
)

df_bairro.limit(50).display()

# COMMAND ----------


# Registra a tabela Delta no catálogo a partir dos dados processados
df_bairro.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable("workspace.gold.tb_Sinistros_Transito_bairro")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Informações por tipo de meio de transporte 
# MAGIC
# MAGIC ###Queremos entender como os diferentes meios de transporte estão associados aos acidentes. Perguntas como: "Qual tipo de veículo está mais envolvido em acidentes?", "Quais apresentam maior número de vítimas fatais?" e outras análises podem ser respondidas com essa tabela.
# MAGIC
# MAGIC ###Nela, cada linha representa um tipo de transporte (como carro, moto, pedestre, etc.), contendo o total de ocorrências em que esteve envolvido e o número total de vítimas fatais associadas.
# MAGIC
# MAGIC ###Essa estrutura permite identificar quais meios estão mais presentes nos acidentes e quais estão relacionados a ocorrências mais graves, servindo como base para análises comparativas e possíveis ações de prevenção.

# COMMAND ----------

# Lista com os meios de transporte
cols_transportes = ["auto", "moto", "ciclom", "ciclista", "pedestre", "onibus", "caminhao", "viatura", "outros"]

# Cria uma lista de DataFrames (um para cada meio de transporte)
dfs_meios = []

for col in cols_transportes:
    # Filtra onde o veículo estava envolvido, calcula os totais e cria as colunas fixas
    df_temp = (
        df_silver
        .filter(F.col(col) > 0)
        .agg(
            F.count("*").alias("total_acidentes"),
            F.sum("vitimasfatais").alias("total_fatais")
        )
        .withColumn("meio_transporte", F.lit(col))
    )
    dfs_meios.append(df_temp)

# Junta todos os DataFrames da lista em um só usando o union
df_meio_de_transporte = dfs_meios[0]
for df_proximo in dfs_meios[1:]:
    df_meio_de_transporte = df_meio_de_transporte.union(df_proximo)

# Calcula a taxa de fatalidade final e ordena pelo maior número de acidentes
df_meio_de_transporte = (
    df_meio_de_transporte
    .withColumn("taxa_fatalidade", F.col("total_fatais") / F.nullif(F.col("total_acidentes"), F.lit(0)))
    .select("meio_transporte", "total_acidentes", "total_fatais", "taxa_fatalidade")
    .orderBy(F.col("total_acidentes").desc())
)

# Exibe o resultado final na tela
df_meio_de_transporte.display()

# COMMAND ----------


# Registra a tabela Delta no catálogo a partir dos dados processados
df_meio_de_transporte.write.format("delta") \
  .mode("overwrite") \
  .option("mergeSchema","true") \
  .saveAsTable("workspace.gold.tb_Sinistros_Transito_transporte")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Queremos entender como os acidentes se distribuem ao longo do dia. Perguntas como: "Em quais horários ocorrem mais acidentes?" e "Existe algum período mais crítico?" podem ser respondidas com essa tabela.
# MAGIC
# MAGIC ###Nela, cada linha representa uma hora do dia, contendo o total de acidentes registrados naquele horário.
# MAGIC
# MAGIC ###Essa estrutura permite identificar padrões temporais, como horários de pico ou períodos de maior risco, servindo como base para análises mais aprofundadas e possíveis ações preventivas.

# COMMAND ----------

df_hora = (
    df_silver
    # Converte a coluna 'hora' para timestamp e extrai apenas a hora do dia (0 a 23)
    .withColumn("hora_dia", F.hour(F.to_timestamp(F.col("hora"), "HH:mm:ss")))
    # Remove linhas onde a hora não pôde ser convertida (equivalente ao dropna)
    .filter(F.col("hora_dia").isNotNull())
    # Agrupa pela hora do dia e conta o total de acidentes
    .groupBy("hora_dia")
    .agg(F.count("hora_dia").alias("total_acidentes"))
    # Ordena o resultado final da hora 0 até a 23
    .orderBy("hora_dia")
)

df_hora.display()

# COMMAND ----------


# Registra a tabela Delta no catálogo a partir dos dados processados
df_hora.write.format("delta") \
  .mode("overwrite") \
  .option("mergeSchema","true") \
  .saveAsTable("workspace.gold.tb_Sinistros_Transito_hora")

# COMMAND ----------

# MAGIC %md
# MAGIC # Aqui, podemos observar que nossos dados são apenas dados parciais entre a madrugada e meio dia. Logo, nossa análise será também limitada.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Agora, queremos entender como os acidentes evoluem ao longo dos meses. Perguntas como: "Qual mês teve mais acidentes?", "Em quais períodos há mais mortes?" e "A taxa de fatalidade varia ao longo do ano?" podem ser respondidas com essa tabela.
# MAGIC
# MAGIC ###Nela, cada linha representa um mês, contendo o total de acidentes, o número de vítimas fatais e a taxa de fatalidade (razão entre vítimas fatais e vítimas totais).
# MAGIC
# MAGIC ###Essa estrutura permite identificar padrões sazonais, comparar períodos mais críticos e avaliar a gravidade dos acidentes ao longo do tempo.

# COMMAND ----------

df_mes = (
    df_silver
    # Agrupa pela coluna de mês
    .groupBy("mes")
    .agg(
        F.count("mes").alias("total_acidentes"),
        F.sum("vitimas").alias("total_vitimas"),
        F.sum("vitimasfatais").alias("total_fatais")
    )
    # Calcula a taxa de fatalidade evitando a divisão por zero caso algum mês não tenha vítimas
    .withColumn("taxa_fatalidade", F.col("total_fatais") / F.nullif(F.col("total_vitimas"), F.lit(0)))
    # Ordena o resultado final do mês menor para o maior 1 a 12
    .orderBy("mes")
)

df_mes.display()

# COMMAND ----------


# Registra a tabela Delta no catálogo a partir dos dados processados
df_mes.write.format("delta") \
  .mode("overwrite") \
  .option("mergeSchema","true") \
  .saveAsTable("workspace.gold.tb_Sinistros_Transito_mes")
