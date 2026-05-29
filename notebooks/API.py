# Databricks notebook source
import requests
import pandas as pd


# COMMAND ----------

url = "https://dados.recife.pe.gov.br/dataset/fdd001d5-05ba-46cd-9edf-5c5a6b58351f/resource/87ac4237-f5f9-44d2-bcf1-927aaa0a2d31/download/acidentes-de-transito-2024.csv"

df = pd.read_csv(url, sep=';')

df.to_csv("data.csv", index=False)

# COMMAND ----------

df

# COMMAND ----------


