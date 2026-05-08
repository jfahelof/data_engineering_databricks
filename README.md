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

### Fonte de dados:

Os dados explorados neste trabalho foram obtidos a partir dos dados públicos de Sinistros de Trânsito com e sem vitimas do ano 2024 da capital pernambucana, Recife, disponibilizados pela [API de Dados Abertos do Recife](https://dados.recife.pe.gov.br/es/dataset/acidentes-de-transito-com-e-sem-vitimas/resource/87ac4237-f5f9-44d2-bcf1-927aaa0a2d31).

No notebook **API.ipynb**, 



