# Pipeline de Engenharia de Dados com Databricks

Neste repositório, implementamos um projeto de engenharia de dados de ponta a ponta utilizando dados de sinistros de trânsito da cidade do Recife obtidos através da [API de Dados Abertos do Recife](https://dados.recife.pe.gov.br/es/dataset/acidentes-de-transito-com-e-sem-vitimas/resource/87ac4237-f5f9-44d2-bcf1-927aaa0a2d31). Os dados apresentados neste projeto correspondem a ocorrências reais de sinistros de trânsito. A análise tem caráter exclusivamente analítico e busca contribuir para discussões relacionadas à mobilidade urbana e segurança no trânsito, sempre com respeito às vítimas e seus familiares. O objetivo deste trabalho é desenvolver uma pipeline de dados baseada na arquitetura Medallion utilizando Databricks, PySpark e Delta Lake, passando pelas camadas Bronze, Silver e Gold até a construção de análises e dashboards analíticos. Durante o desenvolvimento do projeto, foram realizadas etapas de ingestão, limpeza, tratamento, padronização e modelagem dos dados, garantindo maior qualidade, organização e confiabilidade das informações para fins analíticos.


## Arquitetura da pipeline

Na Figura 1, apresentamos a arquitetura da pipeline desenvolvida neste projeto.

<p align="center">
  <img src="imgs/arquitetura.jpg" width="1000">
</p>

<p align="center">
  <em>Figura 1: Arquitetura da pipeline baseada na arquitetura Medallion.</em>
</p>

Na Figura 1, observamos toda arquitetura da pipeline desenvolvida neste projeto. Utilizamos a plataforma Databricks Free Edition devido à sua integração nativa com Apache Spark e Delta Lake, permitindo o desenvolvimento de pipelines de engenharia de dados de forma escalável e organizada. Além disso, a plataforma fornece um ambiente unificado para ingestão, processamento, armazenamento, análise e visualização dos dados, facilitando a implementação da arquitetura Medallion utilizada neste projeto. Ao longo do projeto também utilizamos a linguagem Python e algumas de suas bibliotecas como Pandas, Pyspark e Numpy. Para trabalhar com tabelas, utilizamos SQL. 

Vamos explicar como cada etapa da pipeline foi desenvolvida. 


### Fonte  e extração dos dados

Os dados explorados neste trabalho foram obtidos a partir dos dados públicos de Sinistros de Trânsito com e sem vitimas do ano 2024 da capital pernambucana, Recife, disponibilizados pela [API de Dados Abertos do Recife](https://dados.recife.pe.gov.br/es/dataset/acidentes-de-transito-com-e-sem-vitimas/resource/87ac4237-f5f9-44d2-bcf1-927aaa0a2d31). Utilizamos o notebook **API.ipynb** para fazer a requisição dos dados **data.csv**. 

A partir de agora, entraremos na estrutura medallion do pipeline. Esta estrutura é formada por três camadas, Bronze, Silver e Gold. A estrutura Medallion define como os dados evoluem e como cada camada está relacionada, de modo que a qualidade dos dados aumente. Cada camada será representada por um schema. Vamos descrever o que foi feito em cada camada desta estrutura de forma bem detalhada. 

Para criar os bancos de dados da estrutura medallion, utilizamos o **notebook_01_medallion.ipynb**. Neste notebook, usamos **SQL** para criar cada um dos bancos de dados, bronze, silver e gold. Vamos para a camada Bronze. 


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

A camada Gold da arquitetura medallion é a etapa da pipeline de dados responsável pela disponibilização de dados refinados, agregados e prontos para consumo analítico. Nessa camada, são aplicadas regras de negócio, métricas e transformações que permitem gerar insights estratégicos, dashboards e análises preditivas.

Como o projeto utiliza dados de sinistros de trânsito da cidade do Recife, o foco da camada Gold foi a construção de tabelas analíticas capazes de responder questões relevantes para a mobilidade urbana e segurança no trânsito. Entre os exemplos de análises desenvolvidas estão: bairros com maior número de acidentes, horários de maior ocorrência, tipos de veículos mais envolvidos em sinistros e meios de transporte associados a acidentes fatais.

Além disso, foram criadas métricas agregadas e tabelas otimizadas para visualização em dashboards no Databricks, permitindo uma análise mais eficiente dos padrões de acidentes ao longo do tempo e do espaço urbano. O objetivo final dessa camada é transformar os dados processados em informações úteis para apoiar tomadas de decisão, planejamento urbano e possíveis ações de prevenção de acidentes. Todo processo aplicado na camada Gold está presente no **notebook_04_gold.ipynb**. Agora, vamos para a etapa final da pipeline, Analytics. 
 

## Analytics

A etapa Analytics é a parte final da nossa pipeline de dados, ela é responsável pela exploração visual e interpretação dos dados processados na camada Gold. Nesta etapa, os dados refinados são utilizados para construir gráficos, dashboards e indicadores que auxiliam na identificação de padrões e tendências relacionadas aos sinistros de trânsito em Recife. A camada Analytics permite que gestores, analistas e usuários finais obtenham insights relevantes para apoiar tomadas de decisão e estratégias voltadas à mobilidade urbana e segurança no trânsito.


O produto final do projeto foi um dashboard com algumas das possíveis anélises. Como o objetivo deste trabalho é focado em engenharia de dados, não fora realizado uma análise mais profunda. O dashboard foi construído na plataforma do Databricks. Através do SQL Warehouse do Databricks, foi possível recuperar todas as informações (tabelas) produzidas na camada Gold.  


 <p align="center">
  <img src="imgs/dash recife.png" width="1000">
</p>

<p align="center">
  <em>Figura 2: Construção do Dashboard no Databricks.</em>
</p>

Na Figura 2, observamos a interface da plataforma do Databricks. Na imagem podemos observar o título do dashboard, um total de 5320 acidentes, 5040 vítimas e o número total de 40 vítimas fatais. Logo abaixo temos um gráfico de barras interativo indicando o número total de acidentes por bairro. O restante do dashboard é formado por mais alguns gráficos, iremos discutir os resultados de forma detalhada. 


### Distribuição de acidentes por bairro:

Na Figura 3 temos uma distribuição da participação de cada bairro no número total de acidentes. O gráfico está no formato de pizza e cada fatia representa o percentual de acidentes por bairro. Como podemos observar, Boa Viagem foi o bairro do Recife que mais ocorreu acidentes em 2024, com 10.33% dos acidentes totais. Em segundo, temos Imbiribeira com 5.74% e em terceiro Santo Amaro com 5,57%. O gráfico é interativo e te permite consultar cada bairro encontrado nos dados. 


 <p align="center">
  <img src="imgs/Distribuição de Acidentes por Bairro.png" width="1000">
</p>

<p align="center">
  <em>Figura 3: Distribuição de acidentes por bairro.</em>
</p>



### Total de acidentes por hora:

Na Figura 4, observamos um gráfico de área com o total de acidentes entre as primeiras 12 horas do dia. Aqui podemos observar que os dados estão limitados apenas a estes horários, logo, nossa análise será limitada apenas a estes horários do dia. No entanto, podemos concluir que no período de 1h e 12h, os horários entre 6h e 8h concentram o maior número de acidentes. Nos outros horários temos uma média de 400 acidentes por hora, enquanto que o horários destacado, entre 6h e 8h possuem uma média de 600 acidentes por hora. Isto ocorre devido ao maior fluxo devido ao horário de pico. 


 <p align="center">
  <img src="imgs/Total de acidentes por hora.png" width="1000">
</p>

<p align="center">
  <em>Figura 4: Total de acidentes por hora.</em>
</p>


### Acidentes por meio de transporte:

Por último, iremos fazer uma nálise por meio de transporte. Como podemos observar na Figura 5, carros e motos foram os que mais se envolveram em acidentes, isto ocorre devido ao fato de serem os meios de transporte mais comuns. Outro fator que devemos considerar é o cruzamento de dados, tivemos um total de 5320 acidentes, mas se fizermos uma soma entre o total de acidentes com participação de moto com os acidentes com envolvimento de carros a soma resultará em 6000. Isto ocorre devido as colisões ocorridas, dois ou mais tipos de transporte podem participar de um único acidente. 


 <p align="center">
  <img src="imgs/Total de acidentes por meio de transporte.png" width="1000">
</p>

<p align="center">
  <em>Figura 5: Total de acidentes por meio de transporte.</em>
</p>


Como moto e carro são mais comuns, é natural que causem mais acidentes. Logo, torna-se obrigatório observar a taxa de fatalidade de cada transporte, isto é, a razão entre o número total de acidentes fatais por tipo de veículo e o número total de acidentes causados por tipo de transporte. Quando observamos maior fração de acidentes com vítimas fatais em: caminhão, ciclista, pedestres e "outros". A categoria “outros” representa meios de transporte não classificados nas categorias principais ou registros genéricos presentes na base de dados. 



 <p align="center">
  <img src="imgs/fatalidade meio de transporte.png" width="1000">
</p>

<p align="center">
  <em>Figura 6: Acidentes fatais por meio de transporte.</em>
</p>


Outros gráficos explorados podem ser visualizados no diretório **imgs** deste repositório. 


## Como executar este projeto?

Para executar este projeto, é necessário que você possua uma conta no databricks free edition.

Como o databricks free edition possui algumas limitações, foi necessário rodar o notebook **API.ipynb** localmente para obter os dados no arquivo **data.csv**. Logo após, criamos um diretório no **Workspace** do Databricks e inserimos todos os outros notebooks encontrados no diretório **notebooks** deste repositório. 

Agora, dentro do Databricks, executamos:

### 1: o **notebook_01_medallion** para criar os bancos de dados para cada camada da arquitetura medallion;

### 2: Na aba **Data Ingestion** criamos um volume chamado **dbs** e fazemos a ingestão do arquivo **data.csv**;

### 3: executamos o **notebook_02_bronze**; 

### 4: executamos o **notebook_03_silver**; 

### 5: executamos o **notebook_04_gold**; 

### 6: executamos o **notebook_05_analytics** e poderemos observar alguns gráficos obtidos usando bibliotecas de visualização do Python;

### 7: Finalmente podemos montar nosso dashboard com o **SQL Warehouses**. Temos um arquivo .json neste repositório, você pode realizar o upload na plataforma do databricks. 





