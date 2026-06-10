

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
 
 

# CAMADA BRONZE
# Ingestão dos dados brutos a partir do CSV, com extração de ano/mês/dia
# e registro da data de carga no Data Lake.

 
@dlt.table(
    name="tb_Sinistros_Transito_open_data_bronze_dlt",
    comment="Camada Bronze: dados brutos de sinistros de trânsito do Recife, ingeridos via CSV com enriquecimento de data.",
    table_properties={"quality": "bronze"}
)
def bronze_sinistros():
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .load("/Volumes/workspace/bronze/dbs/data.csv")
    )
 
    # Converte coluna de data e extrai partições temporais
    df = df.withColumn("data", F.to_date("data"))
    df = (
        df
        .withColumn("ano",  F.year("data"))
        .withColumn("mes",  F.month("data"))
        .withColumn("dia",  F.dayofmonth("data"))
        # Auditoria: registra quando os dados foram carregados no Data Lake
        .withColumn("data_carga_lake", F.current_date())
    )
 
    return df
 

# CAMADA SILVER
# Limpeza, remoção de duplicatas, cast de tipos e padronização de nulos.
 
@dlt.table(
    name="tb_Sinistros_Transito_open_data_silver_dlt",
    comment="Camada Silver: dados limpos, sem duplicatas, com tipos corrigidos e nulos padronizados.",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("bairro_nao_nulo", "bairro IS NOT NULL")
def silver_sinistros():
 
    # Lê a tabela Bronze via referência DLT e garante dependência no grafo
    df = dlt.read("tb_Sinistros_Transito_open_data_bronze_dlt")
 
    # 1 Remove duplicatas
    df = df.dropDuplicates()
 
    # 2 Remove colunas com >50% nulos ou sem valor analítico
    colunas_para_remover = [
        "acidente_verificado", "tempo_clima", "situacao_semaforo", "sinalizacao",
        "condicao_via", "conservacao_via", "ponto_controle", "situacao_placa",
        "velocidade_max_via", "mao_direcao", "divisao_via1", "divisao_via2",
        "num_semaforo", "sentido_via", "Protocolo", "detalhe_endereco_acidente", "numero"
    ]
    # Remove apenas as colunas que existirem no DataFrame (proteção contra schema variável)
    colunas_existentes = [c for c in colunas_para_remover if c in df.columns]
    df = df.drop(*colunas_existentes)
 
  
    # 3. Converte colunas numéricas de string para int
    #    Padrão original: "1,0" -> 1,  "2,0" -> 2, etc.
  
    colunas_numericas = [
        "auto", "moto", "ciclom", "ciclista", "pedestre",
        "onibus", "caminhao", "viatura", "outros",
        "vitimas", "vitimasfatais"
    ]
    df = df.select(
        *[
            F.coalesce(
                F.regexp_replace(F.col(c), ",", ".").cast("double").cast("int"),
                F.lit(0)
            ).alias(c)
            if c in colunas_numericas and c in df.columns
            else F.col(c)
            for c in df.columns
        ]
    )
 
    # 4. Padroniza nulos em colunas de texto -> "NA"
    colunas_texto = ["complemento", "bairro", "endereco", "bairro_cruzamento", "tipo"]
    for col_txt in colunas_texto:
        if col_txt in df.columns:
            df = df.withColumn(col_txt, F.coalesce(F.col(col_txt), F.lit("NA")))
 
    return df
 
 
# CAMADA GOLD — Tabela 1: por Bairro
# Total de acidentes, vítimas, vítimas fatais, taxa de fatalidade e
# percentual de acidentes por bairro. Inclui coluna de geolocalização
# formatada para Power BI
 
@dlt.table(
    name="tb_Sinistros_Transito_bairro_gold_dlt",
    comment="Camada Gold: métricas de sinistros agregadas por bairro, com taxa de fatalidade e percentual.",
    table_properties={"quality": "gold"}
)
def gold_bairro():
    df = dlt.read("tb_Sinistros_Transito_open_data_silver_dlt")
 
    df_bairro = (
        df
        .groupBy("bairro")
        .agg(
            F.count("bairro").alias("total_acidentes"),
            F.sum("vitimas").alias("total_vitimas"),
            F.sum("vitimasfatais").alias("total_fatais")
        )
        # Coluna formatada para reconhecimento geográfico no Power BI
        .withColumn(
            "localizacao",
            F.concat(F.col("bairro"), F.lit(", Recife, Pernambuco, Brasil"))
        )
        # Taxa de fatalidade com proteção contra divisão por zero
        .withColumn(
            "taxa_fatalidade",
            F.col("total_fatais") / F.nullif(F.col("total_vitimas"), F.lit(0))
        )
        # Percentual usando Window sem partição (total geral da tabela)
        .withColumn(
            "percentual_acidentes",
            (F.col("total_acidentes") / F.sum("total_acidentes").over(Window.partitionBy())) * 100
        )
        .orderBy(F.col("total_acidentes").desc())
    )
 
    return df_bairro
 
 
# CAMADA GOLD — Tabela 2: por Meio de Transporte
# Para cada modal (auto, moto, ciclista, pedestre etc.): total de acidentes
# em que esteve envolvido e total de vítimas fatais associadas.
 
@dlt.table(
    name="tb_Sinistros_Transito_transporte_gold_dlt",
    comment="Camada Gold: total de acidentes e vítimas fatais por meio de transporte.",
    table_properties={"quality": "gold"}
)
def gold_transporte():
    df = dlt.read("tb_Sinistros_Transito_open_data_silver_dlt")
 
    cols_transportes = [
        "auto", "moto", "ciclom", "ciclista",
        "pedestre", "onibus", "caminhao", "viatura", "outros"
    ]
 
    dfs_meios = []
    for col in cols_transportes:
        if col in df.columns:
            df_temp = (
                df
                .filter(F.col(col) > 0)
                .agg(
                    F.count("*").alias("total_acidentes"),
                    F.sum("vitimasfatais").alias("total_fatais")
                )
                .withColumn("meio_transporte", F.lit(col))
            )
            dfs_meios.append(df_temp)
 
    # União de todos os modais
    df_resultado = dfs_meios[0]
    for df_proximo in dfs_meios[1:]:
        df_resultado = df_resultado.union(df_proximo)
 
    df_resultado = (
        df_resultado
        .withColumn(
            "taxa_fatalidade",
            F.col("total_fatais") / F.nullif(F.col("total_acidentes"), F.lit(0))
        )
        .select("meio_transporte", "total_acidentes", "total_fatais", "taxa_fatalidade")
        .orderBy(F.col("total_acidentes").desc())
    )
 
    return df_resultado
 
 
# CAMADA GOLD — Tabela 3: por Hora do Dia
# Distribuição dos acidentes ao longo das 24 horas do dia.
# Observação: os dados de origem são parciais (madrugada até meio-dia),
# portanto a análise refletirá essa limitação.
 
@dlt.table(
    name="tb_Sinistros_Transito_hora_gold_dlt",
    comment="Camada Gold: total de acidentes por hora do dia (0-23). Dados originais cobrem apenas madrugada até meio-dia.",
    table_properties={"quality": "gold"}
)
def gold_hora():
    df = dlt.read("tb_Sinistros_Transito_open_data_silver_dlt")
 
    df_hora = (
        df
        # Extrai hora inteira (0-23) da string HH:mm:ss
        .withColumn("hora_dia", F.hour(F.to_timestamp(F.col("hora"), "HH:mm:ss")))
        # Remove linhas onde a conversão falhou
        .filter(F.col("hora_dia").isNotNull())
        .groupBy("hora_dia")
        .agg(F.count("hora_dia").alias("total_acidentes"))
        .orderBy("hora_dia")
    )
 
    return df_hora
 
 
# CAMADA GOLD — Tabela 4: por Mês
# Evolução mensal de acidentes, vítimas totais, vítimas fatais e
# taxa de fatalidade ao longo do ano
 
@dlt.table(
    name="tb_Sinistros_Transito_mes_gold_dlt",
    comment="Camada Gold: total de acidentes, vítimas e taxa de fatalidade agregados por mês (1-12).",
    table_properties={"quality": "gold"}
)
def gold_mes():
    df = dlt.read("tb_Sinistros_Transito_open_data_silver_dlt")
 
    df_mes = (
        df
        .groupBy("mes")
        .agg(
            F.count("mes").alias("total_acidentes"),
            F.sum("vitimas").alias("total_vitimas"),
            F.sum("vitimasfatais").alias("total_fatais")
        )
        # Taxa de fatalidade com proteção contra divisão por zero
        .withColumn(
            "taxa_fatalidade",
            F.col("total_fatais") / F.nullif(F.col("total_vitimas"), F.lit(0))
        )
        .orderBy("mes")
    )
 
    return df_mes