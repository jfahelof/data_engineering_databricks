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

A partir de agora, entraremos na estrutura medallion do pipeline. Esta estrutura é formada por três camadas, Bronze, Silver e Gold. A estrutura Medallion define como os dados evoluem e como cada camada está relacionada, de modo que a qualidade dos dados aumente. Cada camada será representada por um catálogo/schema. Vamos descrever o que foi feito em cada camada desta estrutura de forma bem detalhada. 

Para criar os bancos de dados da estrutura medallion, utilizamos o **notebook_01_medallion.ipynb**. Neste notebook, usamos **SQL** para criar cada um dos bancos de dados, bronze, silver, gold e analytics. Vamos para a camada Bronze. 


### Bronze

Nesta etapa do pipeline, a camada Bronze é a primeira da estrutura Medallion. Aqui iremos fazer a ingestão dos dados brutos, isto é, da mesma forma como foi obtido pela API. Nesta etapa, não fazemos nenhum tratamento nos dados. A principal função da camada bronze é servir como uma base para recuperar os dados originais sempre que for necessário. Nesta etapa, adicionamos 4 colunas. Uma delas foi a **data_carga_lake**.  Essa coluna registra quando os dados foram carregados no Data Lake, sendo essencial para auditoria, rastreabilidade e controle de versões dos dados. As outras três foram apenas uma extração de dia, mês e ano da coluna **data** dos dados originais. 

Feito isso, salvamos os dados brutos na camada Bronze. Nesta etapa, os dados são gravados utilizando o formato Delta Lake, que é construído sobre arquivos Parquet. O Parquet é um formato de armazenamento colunar, permitindo maior eficiência na compressão e melhor desempenho em consultas, pois apenas as colunas necessárias são lidas durante o processamento. O Delta Lake adiciona funcionalidades importantes ao Parquet, como controle de versão, transações ACID e suporte a cargas incrementais. Isso garante maior confiabilidade no processamento de dados, evitando inconsistências e permitindo operações como inserção, atualização e merge de dados.

Além disso, será criada uma tabela externa no catálogo do Databricks, apontando para os arquivos armazenados no Data Lake. Dessa forma, os dados podem ser consultados via SQL sem a necessidade de duplicação, facilitando a integração com ferramentas analíticas e de visualização. Com essa abordagem, obtemos um pipeline mais eficiente, escalável e confiável para o processamento de grandes volumes de dados. Todo este processo da camada Bronze está presente no **notebook_02_bronze.ipynb**. Agora, iremos para a camada Silver. 


### Silver

Nesta etapa da pipeline, os dados passaram por um tratamento profundo de refinamento. Para entender todo o processo realizado nesta etapa vamos falar um pouco da estrutura dos dados obtidos. Os dados da camada Bronze foram inicialmente carregados com 41 colunas e 5315 registros. Como os dados foram obtidos diretamente da API pública, a maior parte das colunas foi inicialmente interpretada como `object`, exigindo posteriormente um processo de tipagem e padronização na camada Silver. Um exemplo da estrutura dos dados está mostrado na tabela abaixo:

| Coluna      | Tipo inicial |
|--------------|--------------|
| Protocolo    | object |
| data         | object |
| hora         | object |
| bairro       | object |
| vitimas      | object |
| ano          | int32 |
| mes          | int32 |
| dia          | int32 |

Nesta etapa é fundamental uma análise exploratória. Primeiro verificamos se havia linhas duplicadas, mas não existiam. Logo após, detectamos colunas totalmente vazias ou com menos de 50% dos dados devidamente preenchidos. Removemos todas estas colunas, pois nenhuma informação útil poderia ser encontrda com elas. Para garantir total privacidade dos dados, removemos os identificadores de protocolo. Algumas colunas que deveriam ser preenchidas com números inteiros, estavam declaradas como string. Por exemplo, algo que deveriam ser 0, 1, ou 2, estavam escritas com vírgula, ou seja, 0,0, 1,0, 2,0. Além de ocupar muita memória, armazenar os dados desta forma também atrapalham em análises numéricas, logo, fizemos uma convenção de 0,0 -> 0, 1,0 -> 1 e assim por diante. Por fim, padronizamos os valores nulos encontrados em algumas colunas, entradas que não estavam preenchidas, colocamos 'NA'. 

Após o processo de tipagem e limpeza dos dados, observamos uma redução superior a 40% no tamanho do DataFrame em memória. Essa otimização melhora a eficiência no armazenamento e processamento dos dados, reduzindo o consumo de memória e aumentando o desempenho das operações analíticas realizadas ao longo da pipeline. Logo após, fizemos a persistência dos dados na camada Silver em formato Delta Lake (Parquet). 

Embora o cenário atual não envolva ingestão incremental, a utilização do Delta Lake garante que o pipeline esteja preparado para evoluções futuras, mantendo escalabilidade e robustez. Após a gravação dos dados, será criada uma tabela no catálogo do Databricks (Unity Catalog), permitindo consultas via SQL diretamente sobre os dados armazenados no Data Lake, sem necessidade de duplicação. Essa abordagem assegura um fluxo de dados eficiente, confiável e alinhado com boas práticas de engenharia de dados. Todo este processo da camada Silver está presente no **notebook_03_silver.ipynb**. Agora, vamos para a camada Gold. 


## Gold


 



 
