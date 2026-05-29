# Databricks notebook source
# MAGIC %md
# MAGIC # Arquitetura Medallion
# MAGIC
# MAGIC ### Vamos criar os banco de dados (schemas) para cada camada da estrutura medallion: 
# MAGIC
# MAGIC ## **Bronze** -> **Silver** -> **Gold** -> **Analytics**. 
# MAGIC
# MAGIC ### Onde:
# MAGIC
# MAGIC ### **Bronze**: ingestão de dados brutos.
# MAGIC ### **Silver**: limpeza, tratamento e padronização.
# MAGIC ### **Gold**: tabelas analíticas e métricas.

# COMMAND ----------

# MAGIC %sql
# MAGIC  -- Cria um banco de dados para a camada Bronze
# MAGIC  CREATE DATABASE IF NOT EXISTS bronze; 
# MAGIC
# MAGIC  -- Cria um banco de dados para a camada Silver
# MAGIC  CREATE DATABASE IF NOT EXISTS silver; 
# MAGIC
# MAGIC  -- Cria um banco de dados para a camada Gold
# MAGIC  CREATE DATABASE IF NOT EXISTS gold;
# MAGIC
