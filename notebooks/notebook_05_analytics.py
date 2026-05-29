# Databricks notebook source
# MAGIC %md
# MAGIC # Analytics
# MAGIC
# MAGIC Este notebook corresponde à **camada Analytics** do pipeline de dados, sendo responsável pela visualização e análises de dados. É a etapa da pipeline que consome o produto gerado na camada Gold. A partir destas análise, teremos um melhor entendimento dos dados e poderemos realizar predições. 
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC ### Primeiro iremos ler as tabelas da camada gold e visualizar para assegurar que estão corretas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Informação por bairro:

# COMMAND ----------


df_bairro_pandas = spark.read.table(
    "workspace.gold.tb_Sinistros_Transito_bairro"
).toPandas()

df_bairro_pandas.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Informação por tipo de transporte:

# COMMAND ----------


df_transporte_pandas = spark.read.table(
    "workspace.gold.tb_Sinistros_Transito_transporte"
).toPandas()

df_transporte_pandas.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Informação por hora do dia:

# COMMAND ----------

df_hora_pandas = spark.read.table(
    "workspace.gold.tb_Sinistros_Transito_hora"
).toPandas()

df_hora_pandas.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Informação por mês do ano de 2024:

# COMMAND ----------

df_mes_pandas = spark.read.table(
    "workspace.gold.tb_Sinistros_Transito_mes"
).toPandas()

df_mes_pandas.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agora, iremos visualizar em gráficos para facilitar a 

# COMMAND ----------

import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

fig = px.bar(
    df_bairro_pandas.sort_values(by="total_acidentes"),
    x="total_acidentes",
    y="bairro",
    orientation="h",
    text="total_acidentes",
    hover_data={
        "total_vitimas": True,
        "total_fatais": True,
        "taxa_fatalidade": ':.2f'
    },
    title="Acidentes por Bairro"
)

fig.update_layout(
    xaxis_title="Número de Acidentes",
    yaxis_title="Bairro",
    height=800 
)

fig.show()

# COMMAND ----------

plt.plot(df_hora_pandas["hora_dia"], df_hora_pandas["total_acidentes"], 'bs-', linewidth=2)
plt.title("Acidentes por Hora", fontsize=20)
plt.xlabel("Hora", fontsize=20)
plt.xticks(fontsize=14)
plt.yticks(fontsize=14)
plt.ylabel("Número de Acidentes", fontsize=20)
plt.show()

# COMMAND ----------



# COMMAND ----------

plt.bar(df_transporte_pandas["meio_transporte"], df_transporte_pandas["total_acidentes"], color="tomato")
plt.title("Acidentes por Meio de Transporte", fontsize=20)
plt.xlabel("Meio de Transporte", fontsize=20)
plt.xticks(fontsize=14, rotation = 90)
plt.yticks(fontsize=14)
plt.ylabel("Número de Acidentes", fontsize=20)
plt.show()

# COMMAND ----------

plt.bar(df_transporte_pandas["meio_transporte"], df_transporte_pandas["taxa_fatalidade"] * 100, color="darkorange")
plt.title("Taxa de Fatalidade por Meio de Transporte", fontsize=20)
plt.xlabel("Meio de Transporte", fontsize=20)
plt.xticks(fontsize=14, rotation = 70)
plt.yticks(fontsize=14)
plt.ylabel("Taxa de Fatalidade %", fontsize=20)
plt.show()

# COMMAND ----------

plt.bar(df_mes_pandas["mes"], df_mes_pandas["total_acidentes"], color="darkorange")
plt.title("Acidentes por Mês", fontsize=20)
plt.xlabel("Mês", fontsize=20)
plt.xticks(fontsize=14)
plt.yticks(fontsize=14)
plt.ylabel("Número de Acidentes", fontsize=20)
plt.show()


# COMMAND ----------

plt.bar(df_mes_pandas["mes"], df_mes_pandas["taxa_fatalidade"] * 100, color="darkred")
plt.title("Taxa de Fatalidade por Mês", fontsize=20)
plt.xlabel("Mês", fontsize=20)
plt.xticks(np.arange(1, 13), fontsize=14) 
plt.yticks(fontsize=14)
plt.ylabel("Taxa de Fatalidade %", fontsize=20)
plt.show()

# COMMAND ----------


