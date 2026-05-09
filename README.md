# Pipeline de Engenharia de Dados com Databricks

Neste repositório, implementamos um projeto de engenharia de dados de ponta a ponta utilizando dados de sinistros de trânsito da cidade do Recife obtidos através da [API de Dados Abertos do Recife](https://dados.recife.pe.gov.br/es/dataset/acidentes-de-transito-com-e-sem-vitimas/resource/87ac4237-f5f9-44d2-bcf1-927aaa0a2d31). O objetivo deste trabalho é desenvolver uma pipeline de dados baseada na arquitetura Medallion utilizando Databricks, PySpark e Delta Lake, passando pelas camadas Bronze, Silver e Gold até a construção de análises e dashboards analíticos. Durante o desenvolvimento do projeto, foram realizadas etapas de ingestão, limpeza, tratamento, padronização e modelagem dos dados, garantindo maior qualidade, organização e confiabilidade das informações para fins analíticos.


## Arquitetura da pipeline

Na Figura 1, apresentamos a arquitetura da pipeline desenvolvida neste projeto.

<p align="center">
  <img src="imgs/arquitetura.jpg" width="1000">
</p>

<p align="center">
  <em>Figura 1: Arquitetura da pipeline baseada na arquitetura Medallion.</em>
</p>

Na Figura 1, observamos no topo as principais tecnologias utilizadas. Utilizamos a plataforma Databricks Free Edition devido à sua integração nativa com Apache Spark e Delta Lake, permitindo o desenvolvimento de pipelines de engenharia de dados de forma escalável e organizada. Além disso, a plataforma fornece um ambiente unificado para ingestão, processamento, armazenamento, análise e visualização dos dados, facilitando a implementação da arquitetura Medallion utilizada neste projeto. Ao longo do projeto também utilizamos a linguagem Python e algumas de suas bibliotecas como Pandas, Pyspark e Numpy. Para trabalhar com tabelas, utilizamos SQL. 

Vamos explicar como cada etapa da pipeline foi desenvolvida. 

### Fonte de dados

Os dados explorados neste trabalho foram obtidos a partir dos dados públicos de Sinistros de Trânsito com e sem vitimas do ano 2024 da capital pernambucana, Recife, disponibilizados pela [API de Dados Abertos do Recife](https://dados.recife.pe.gov.br/es/dataset/acidentes-de-transito-com-e-sem-vitimas/resource/87ac4237-f5f9-44d2-bcf1-927aaa0a2d31). Utilizamos o notebook **API.ipynb** para fazer a requisição dos dados **data.csv**. 

A partir de agora, entraremos na estrutura medallion do pipeline. Esta estrutura é formada por três camadas, Bronze, Silver e Gold. A estrutura Medallion define como os dados evoluem e como cada camada está relacionada, de modo que a qualidade dos dados aumente. Cada camada será representada por um banco de dados. Vamos descrever o que foi feito em cada camada desta estrutura de forma bem detalhada. 

Para criar os bancos de dados da estrutura medallion, utilizamos o **notebook_01_medallion.ipynb**. Neste notebook, usamos **SQL** para criar cada um dos bancos de dados, bronze, silver, gold e analytics. Vamos para a camada Bronze. 

### Bronze

Nesta etapa do pipeline, a camada Bronze é a primeira da estrutura Medallion. Aqui iremos fazer a ingestão dos dados brutos, isto é, da mesma forma como foi obtido pela API. Nesta etapa, não fazemos nenhum tratamento nos dados. A principal função da camada bronze é servir como uma base para recuperar os dados originais sempre que for necessário. Nesta etapa, adicionamos 4 colunas. Uma delas foi a **data_carga_lake**.  Essa coluna registra quando os dados foram carregados no Data Lake, sendo essencial para auditoria, rastreabilidade e controle de versões dos dados. As outras três foram apenas uma extração de dia, mês e ano da coluna **data** dos dados originais. 

Feito isso, salvamos os dados brutos na camada Bronze. Nesta etapa, os dados são gravados utilizando o formato Delta Lake, que é construído sobre arquivos Parquet. O Parquet é um formato de armazenamento colunar, permitindo maior eficiência na compressão e melhor desempenho em consultas, pois apenas as colunas necessárias são lidas durante o processamento. O Delta Lake adiciona funcionalidades importantes ao Parquet, como controle de versão, transações ACID e suporte a cargas incrementais. Isso garante maior confiabilidade no processamento de dados, evitando inconsistências e permitindo operações como inserção, atualização e merge de dados.

Além disso, será criada uma tabela externa no Databricks SQL Warehouse, apontando para os arquivos armazenados no Data Lake. Dessa forma, os dados podem ser consultados via SQL sem a necessidade de duplicação, facilitando a integração com ferramentas analíticas e de visualização. Com essa abordagem, obtemos um pipeline mais eficiente, escalável e confiável para o processamento de grandes volumes de dados.



