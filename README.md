# ![logo](logo_gov_ce.png)

# Projeto Análise Convênios e Contratos do Governo do Estado do Ceará

Este repositório contém todos os códigos e documentação para o projeto de engenharia de dados e análise de convênios e contratos do Governo do Ceará, extraídos do Portal de Dados Abertos.

## Índice

- [**Engenharia de Dados com Airflow**](#engenharia-de-dados-com-airflow)
    - [Airflow e Docker](#airflow-e-docker)
    - [Banco de Dados PostgreSQL com DBeaver](#banco-de-dados-postgresql-com-dbeaver)
    - [DAGs do Airflow](#dags-do-airflow)
    - [Execução Manual e Agendamento](#execução-manual-e-agendamento)
    - [Automação do Pipeline de Dados com Airflow DAGs](#automação-do-pipeline-de-dados-com-airflow-dags)
- [**Desenvolvimento dos Scripts Python para Automação de Tarefas**](#desenvolvimento-dos-scripts-python-para-automação-de-tarefas)
    - [Características dos Scripts](#características-dos-scripts)
    - [Notebooks Jupyter para Análise e Educação](#notebooks-jupyter-para-análise-e-educação)
- [**Dicionário de Dados Stage**](#dicionário-de-dados-stage)
- [**Criação do Banco de Dados Airflow (Schema Stage)**](#criação-do-banco-de-dados-airflow-schema-stage)
- [**Análise Exploratória Inicial no Banco de Dados Stage**](#análise-exploratória-inicial-no-banco-de-dados-stage)
- [**Processo de ETL no Banco de Dados Stage**](#processo-de-etl-no-banco-de-dados-stage)
- [**Dicionário de Dados DW**](#dicionário-de-dados-dw)
- [**Modelagem de Dados do Data Warehouse**](#modelagem-de-dados-do-data-warehouse)
- [**Estruturação do Data Warehouse (Schema DW)**](#estruturação-do-data-warehouse-schema-dw)
- [**Análise Exploratória e Estatística DW**](#análise-exploratória-e-estatística-dw)
- [**Data Visualization**](#data-visualization)
- [**Licença**](#licença)
- [**Como usar este repositório**](#como-usar-este-repositório)
- [**Equipe**](#team)
- [**🎁 Expressões de Gratidão**](#Words-of-Appreciation)

## Engenharia de Dados com Airflow

Neste projeto, utilizamos o Apache Airflow, uma plataforma de código aberto para orquestração de workflows, para automatizar e gerenciar o pipeline de ETL (Extração, Transformação e Carregamento) dos dados dos convênios e contratos do Governo do Estado do Ceará. O Airflow permite definir, agendar e monitorar workflows de forma programática, oferecendo uma interface de usuário que facilita o gerenciamento das tarefas de dados.

### Airflow e Docker

Para garantir a portabilidade e a facilidade na configuração do ambiente, o Airflow é executado dentro de contêineres Docker. O Docker encapsula o Airflow e suas dependências em contêineres isolados, permitindo que o serviço seja executado de maneira consistente em qualquer ambiente de hospedagem que suporte Docker. Isso simplifica o processo de implantação e configuração do Airflow, além de proporcionar a separação entre o ambiente de execução e o sistema operacional subjacente.


### Banco de Dados PostgreSQL com DBeaver

Nossa infraestrutura de back-end foi feita no PostgreSQL, um sistema de banco de dados relacional de código aberto renomado por sua robustez e confiabilidade. Para gerenciar e interagir com o nosso banco de dados PostgreSQL, adotamos o DBeaver, um cliente SQL universal que oferece uma interface gráfica rica para o gerenciamento de bancos de dados. Com o DBeaver, temos uma ferramenta poderosa para realizar consultas SQL, visualizar a estrutura de dados, desenvolver esquemas de banco de dados e depurar operações, tudo isso contribuindo para um processo de ETL eficiente e confiável no Airflow.

As características avançadas do PostgreSQL, como conformidade com ACID, suporte a transações complexas e extensibilidade, fazem dele a escolha ideal para armazenar e manipular os dados críticos do nosso projeto. Ao combinar o PostgreSQL com as funcionalidades intuitivas do DBeaver, maximizamos a produtividade e garantimos a manutenção eficaz do nosso ambiente de dados.


### DAGs do Airflow

O Airflow utiliza DAGs (Directed Acyclic Graphs) para definir a sequência e as dependências das tarefas de ETL. Cada DAG representa um conjunto de tarefas que são executadas em uma ordem específica, permitindo uma complexidade arbitrária na orquestração das tarefas de dados. As DAGs são definidas em Python, o que proporciona flexibilidade e poder na criação de pipelines de dados personalizados para atender às necessidades específicas do projeto.

### Execução Manual e Agendamento

Embora as DAGs possam ser agendadas para execução em intervalos regulares, também oferecemos a opção de execução manual. Isso é útil para situações em que uma execução ad-hoc do pipeline é necessária, como na realização de testes ou na manipulação de dados após uma atualização de esquema. A execução manual é realizada por meio da interface web do Airflow, fornecendo um meio conveniente e direto para disparar pipelines conforme necessário.

---

Para começar a usar o Airflow com Docker neste projeto, se faz necessário fazer as configurações de ambas ferramentas siga as instruções detalhadas de configuração disponíveis no site do Airflow[https://airflow.apache.org/] e Docker[https://www.docker.com/]. A documentação inclui todas as etapas necessárias para inicializar o Airflow em um ambiente Docker e para configurar, executar e monitorar as DAGs de ETL.


### Automação do Pipeline de Dados com Airflow DAGs

A complexidade e a vastidão dos dados requerem um sistema robusto para orquestrar o pipeline de ETL, garantindo que as diversas tarefas sejam executadas de forma sequencial e confiável. Para atender a essa necessidade, implementamos Directed Acyclic Graphs (DAGs) no Apache Airflow, estruturando assim um sistema de automação que gerencia a execução de tarefas desde a extração de dados até a análise final. Abaixo, detalhamos as quatro DAGs principais que compõem nosso pipeline de dados:

#### 1. DAG de Limpeza de Dados (`limpeza_dados`)

- **Objetivo**: Automatizar a exclusão de todas as tabelas dos bancos de dados Stage e Data Warehouse. Essa DAG é particularmente útil para resetar o ambiente de dados e prepará-lo para novos ciclos de teste ou execução de ETL.
- **Processo**: Executa scripts SQL que limpam os schemas `stage` e `dw`, removendo tabelas e seus dados. Isso assegura um ponto de partida limpo para operações subsequentes, eliminando a necessidade de intervenção manual e prevenindo inconsistências de dados.

#### 2. DAG de ETL para o Banco de Dados Stage (`etl_dados_stage`)

- **Objetivo**: Orquestrar a extração de dados das APIs, seguida pela criação de tabelas no banco de dados Stage, inserção dos dados extraídos e execução de procedimentos de ETL específicos para as tabelas de Convênios e Contratos.
- **Processo**: Esta DAG automatiza desde a coleta de dados até a preparação inicial dos mesmos, incluindo a remoção de duplicatas baseada em chaves únicas, o tratamento de valores nulos e a normalização de formatos de texto.

#### 3. DAG de ETL para Dimensões e Fatos (`etl_dimensao_fatos`)

- **Objetivo**: Executar os scripts de ETL que conectam ao banco de dados Stage, criam tabelas de dimensão e fato no Data Warehouse e inserem dados nessas tabelas.
- **Processo**: Essencial para estruturar o Data Warehouse, essa DAG assegura a correta transferência e transformação dos dados do ambiente de Stage para o DW, promovendo a integração entre as diversas dimensões e fatos de nosso modelo de dados.

#### 4. DAG de Envio de Email com Análises (`enviar_link_analises`)

- **Objetivo**: Enviar automaticamente um email contendo links para a documentação e análises do projeto, oferecendo uma forma rápida e eficiente de compartilhar os resultados das análises com stakeholders.
- **Processo**: Essa DAG é configurada para disparar emails após a conclusão bem-sucedida das outras DAGs de ETL, garantindo que os interessados sejam informados das últimas atualizações e análises disponíveis.

### Conclusão

As DAGs do Airflow representam uma peça fundamental na automação e na gestão eficiente de nosso pipeline de dados, desde a limpeza e preparação inicial dos dados até a análise final e compartilhamento dos insights. Ao implementar essas DAGs, maximizamos a eficiência operacional, reduzimos a possibilidade de erros manuais e garantimos a entrega oportuna de dados confiáveis e insights valiosos.


### Desenvolvimento dos Scripts Python para Automação de Tarefas

O foco principal da nossa infraestrutura de dados são os scripts Python, que automatizam integralmente as etapas críticas de nosso pipeline de dados. Esses scripts são projetados com um foco preciso em eficiência, clareza e reutilização, permitindo que realizemos desde a extração de dados de APIs externas até o carregamento desses dados em nossos bancos de dados Stage e Data Warehouse, passando pelo tratamento e transformação dos dados.

#### Características dos Scripts

- **Extração de Dados**: Utilizamos requests HTTP para extrair dados diretamente de APIs públicas, empregando métodos que garantem a obtenção completa e precisa dos conjuntos de dados necessários.
  
- **Criação Dinâmica de Tabelas**: Nossos scripts verificam a estrutura dos dados extraídos e, com base nessa estrutura, criam tabelas no banco de dados que se alinham perfeitamente com os formatos dos dados, assegurando assim uma integração sem falhas.

- **Inserção e Tratamento de Dados**: Além de inserir os dados nas tabelas correspondentes, nossos scripts implementam várias rotinas de tratamento de dados, incluindo a limpeza de valores nulos, a remoção de duplicatas e a normalização de formatos de dados.

#### Notebooks Jupyter para Análise e Educação

Reconhecendo a importância da transparência e da aprendizagem em projetos de engenharia de dados, também disponibilizamos notebooks Jupyter que detalham cada passo do processo de ETL. Estes notebooks servem a dupla função de documentar nosso processo de maneira interativa e de fornecer uma ferramenta educacional para aqueles que buscam entender melhor as nuances da engenharia de dados e análise de ados.

- **Exploração de Dados**: Os notebooks Jupyter fornecem um ambiente excelente para a exploração inicial de dados, permitindo a visualização imediata dos dados extraídos e a execução de análises exploratórias básicas.

- **Tutorial Passo a Passo**: Cada notebook acompanha o leitor através das etapas lógicas do processamento de dados, desde a extração e a limpeza até a análise preliminar, oferecendo insights sobre as decisões tomadas em cada fase.

- **Flexibilidade e Interatividade**: Com a interatividade fornecida pelos notebooks Jupyter, os usuários podem modificar e executar trechos de código para experimentar com os dados, proporcionando uma compreensão prática dos princípios da engenharia de dados.


## Dicionário de Dados Stage

Especificação dos campos e tipos de dados utilizados nas tabelas do banco de dados Stage, com detalhes sobre a origem e o contexto dos dados.

## Criação do Banco de Dados Airflow (Schema Stage)

O Banco de Dados Airflow no Schema Stage foi criado para ser o nosso Banco de Dados Transacional, onde podemos executar as operações de análises dos dados e tratamentos, sem ter que utilizar o banco de produção.
No Stage, temos duas tabelas, que correspondem as tabelas de Convênios e Contratos.

Na execução do Script, contém uma função que cria um dicionário ds páginas da API, o qual é possível escolher os números páginas para extração. Nesse projeto escolhemos 10 páginas.

## Análise Exploratória Inicial no Banco de Dados Stage

Antes de avançar para as complexidades do Data Warehouse, é essencial realizar uma análise exploratória abrangente dos dados coletados no banco de dados Stage. Utilizando notebooks Jupyter como nossa ferramenta de investigação, mergulhamos profundamente nos dados para identificar padrões, anomalias e insights preliminares. Esta análise inicial é um passo fundamental para assegurar a qualidade e a relevância dos dados antes de sua transformação e carregamento no ambiente do Data Warehouse.

### Pontos Focais da Análise

Em nossa exploração, damos especial atenção a várias dimensões críticas que influenciam diretamente a utilidade dos dados para análises subsequentes:

- **Valores Nulos**: Identificamos colunas com uma alta incidência de valores nulos, avaliando o impacto dessas ausências nos dados e determinando estratégias de imputação ou exclusão conforme apropriado. Compreender a natureza dos dados faltantes é crucial para garantir análises precisas.

- **Registros Duplicados**: A detecção e remoção de duplicatas são essenciais para evitar distorções nas análises. Empregamos técnicas rigorosas para identificar registros duplicados com base em chaves únicas, assegurando a integridade dos dados.

- **Normalização de Tipos de Dados**: Avaliamos os tipos de dados atribuídos a cada coluna, corrigindo discrepâncias e convertendo dados para formatos mais apropriados para análise. Esta etapa é vital para facilitar operações de dados subsequentes, especialmente aquelas envolvendo cálculos ou comparações temporais.

- **Análise de Texto e Categorização**: Examinamos campos de texto para identificar a necessidade de normalização ou categorização, o que pode incluir a padronização de termos, a extração de informações relevantes ou a conversão de texto livre em categorias pré-definidas.

### Ferramentas e Técnicas

A análise é conduzida com uma combinação de ferramentas de processamento de dados e visualização em Python, incluindo bibliotecas como Pandas para manipulação de dados e Matplotlib e Seaborn para visualização. Essas ferramentas nos permitem explorar de forma eficaz os conjuntos de dados, identificando tendências, padrões e potenciais áreas de interesse para análises mais profundas no DW.

### Impacto na Modelagem do Data Warehouse

Os insights obtidos nesta fase inicial de análise exploratória são fundamentais para informar a modelagem do nosso Data Warehouse. Decisões sobre a estrutura de tabelas, a necessidade de dimensões adicionais e a forma de tratar dados históricos são todas influenciadas pelas descobertas feitas durante esta etapa. Ao assegurar uma compreensão sólida dos dados em seu estado inicial, podemos projetar um DW que não apenas acomoda os dados de maneira eficiente, mas também maximiza sua utilidade para análises complexas e geração de insights.


## Processo de ETL no Banco de Dados Stage

O processo de ETL (Extração, Transformação e Carregamento) é uma etapa crucial no nosso pipeline de dados, preparando as informações extraídas para análises futuras. Utilizando scripts Python detalhadamente elaborados, baseamos nosso tratamento de dados em descobertas e análises preliminares realizadas através de notebooks Jupyter. Este processo não só garante a qualidade e a integridade dos dados no banco de dados Stage, mas também estabelece uma base sólida para as etapas subsequentes de modelagem e análise no Data Warehouse.

### Limpeza e Normalização dos Dados

Nosso script de ETL engloba várias operações essenciais para a preparação dos dados, incluindo:

- **Exclusão de Duplicatas**: Identificamos e removemos registros duplicados baseando-nos em chaves únicas específicas para cada conjunto de dados. Esta etapa é fundamental para garantir a unicidade e a precisão dos dados analisados.

- **Tratamento de Valores Nulos**: Avaliamos cuidadosamente os campos com valores nulos, aplicando estratégias de tratamento adequadas que variam desde a imputação de valores até a exclusão de registros, dependendo do contexto e da importância dos dados em questão.

- **Normalização de Tipos de Dados**: Convertendo tipos de dados inadequados para formatos mais apropriados para análise. Um exemplo é a transformação de datas armazenadas como texto para o formato `TIMESTAMP WITH TIME ZONE`, facilitando operações temporais complexas e aumentando a precisão das consultas temporais.

### Consolidação da Base de Dados

Após o tratamento, os dados são consolidados em um formato estruturado e coerente, pronto para ser transferido para o Data Warehouse. Este processo assegura que o fluxo de dados do Stage para o DW seja suave e eficiente, maximizando a utilidade das informações para análises e decisões baseadas em dados.

### Importância do Processo de ETL

O meticuloso processo de ETL no banco de dados Stage é um componente vital do nosso projeto de engenharia de dados. Ele não apenas melhora a qualidade dos dados disponíveis para análise, mas também otimiza o desempenho do sistema e apoia decisões informadas. Através deste processo, estamos comprometidos em fornecer uma base de dados confiável, precisa e acessível para todos os stakeholders envolvidos no projeto de análise de convênios e contratos do Governo do Estado do Ceará.


## Dicionário de Dados DW

Definição detalhada dos elementos constituintes do Data Warehouse, com explicação sobre a modelagem e relacionamentos entre as tabelas. Nessa etapa algumas colunas foram desconsideradas, por não serem relevantes nas análises ou por conter valores totalmente nulos.

### Modelagem de Dados do Data Warehouse

Para o desenho e a implementação do modelo de dados do nosso Data Warehouse, adotamos uma abordagem estratégica que prioriza a eficiência na análise de dados, a integridade referencial e a escalabilidade. Entendendo a complexidade e a vastidão dos dados envolvidos nos convênios e contratos do Governo do Estado do Ceará, optamos por um modelo que favorece a flexibilidade analítica e o desempenho em consultas.

#### Ferramenta de Modelagem: DB Designer

Para facilitar a visualização e o planejamento da nossa arquitetura de dados, escolhemos o DB Designer como nossa ferramenta de modelagem. Este software nos permitiu criar um esquema visual do Data Warehouse, evidenciando as relações entre tabelas, as chaves primárias e estrangeiras, e garantindo que o design do banco de dados esteja alinhado com as melhores práticas de modelagem dimensional.

#### Estratégia de Modelagem

Nossa modelagem segue o paradigma dimensional, uma escolha que se destaca por sua simplicidade e eficácia em ambientes de Data Warehouse. Neste modelo, separamos os dados em duas categorias principais:

- **Dimensões (Dim)**: Tabelas que contêm atributos descritivos, servindo como pontos de referência e filtros nas análises. As dimensões criadas (`dim_contrato`, `dim_entidade`, `dim_modalidade`, `dim_participacao`, `dim_projeto`) oferecem uma visão multidimensional dos dados, facilitando o entendimento e a segmentação dos mesmos.

- **Fatos (Fato)**: Tabelas que registram as métricas quantitativas e as transações, essenciais para análises e relatórios. As tabelas `fato_contratos` e `fato_convenios` acumulam os dados numéricos e os eventos relacionados a cada contrato e convênio, respectivamente.

#### Justificativa das Decisões

A escolha de unificar algumas dimensões, enquanto mantemos tabelas fato separadas, foi guiada pela necessidade de simplificar as consultas sem comprometer a riqueza e a precisão das análises. Essa estrutura permite que analistas e gestores explorem os dados de maneira intuitiva, cruzando informações entre diferentes dimensões para extrair insights precisos e acionáveis.

O uso do DB Designer, além de otimizar nosso processo de modelagem, assegurou que todas as partes interessadas pudessem compreender e contribuir para o design do Data Warehouse, garantindo que a solução final atendesse plenamente às necessidades do projeto.

Ao final, nossa abordagem na construção do modelo de dados do Data Warehouse demonstra um compromisso com a clareza, a eficiência e a adaptabilidade, assegurando que o ambiente analítico possa evoluir juntamente com as demandas do Governo do Estado do Ceará e das partes interessadas.


## Estruturação do Data Warehouse (Schema DW)

O Schema DW no nosso banco de dados Airflow foi projetado como estrutura principal do nosso projeto. Inicialmente, consideramos a implantação do DW em uma plataforma cloud, como o Google BigQuery, para aproveitar a escalabilidade e o desempenho. Contudo, devido às considerações logísticas e ao tempo necessário para a migração de dados, essa ideia permanece como um plano futuro a ser implementado.

No Data Warehouse, estruturamos uma série de tabelas fundamentais para a organização e análise dos dados:

- `dim_contrato`: Armazena informações detalhadas sobre cada contrato.
- `dim_entidade`: Contém dados sobre as entidades envolvidas.
- `dim_modalidade`: Registra as modalidades de contratos e convênios.
- `dim_participacao`: Detalha a participação das entidades nos contratos.
- `dim_projeto`: Fornece informações sobre os projetos associados.
- `fato_contratos`: Tabela fato que compila dados transacionais de contratos.
- `fato_convenios`: Similarmente, agrupa informações transacionais de convênios.

Observamos que as estruturas das tabelas de Convênios e Contratos compartilham colunas idênticas, o que nos levou a unificar as dimensões, enquanto mantivemos tabelas fato distintas para cada tipo de dado. Essa decisão estratégica não só otimizou nosso Data Warehouse ao reduzir a redundância de tabelas, mas também melhorou a eficiência da consulta de dados, permitindo análises precisas por meio das relações estabelecidas entre as dimensões e as tabelas fato.relacionamentos com as dimensões. Isos otimiza a nossa Data Warehouse, diminuindo a quantidade de tabelas no banco de dados.


## Análise Exploratória e Estatística DW

Nesta etapa do projeto, analisamos os dados inseridos no Data Warehouse. Utilizando técnicas de análise de dados e estatíticas com o objetivo de desvendar padrões, identificar correlações e destacar anomalias nos convênios e contratos gerenciados pelo Governo do Estado do Ceará.

As análises realizadas forneceram insights valiosos sobre a eficácia das políticas públicas, a distribuição de recursos e as tendências de gastos, contribuindo assim para a tomada de decisões estratégicas baseadas em evidências. Além disso, essas investigações estatísticas pavimentaram o caminho para a identificação de oportunidades de melhoria nos processos de governança e fiscalização, garantindo maior transparência e eficiência na gestão dos recursos públicos.


## Data Visualization

Para dar vida aos dados e facilitar a interpretação das análises realizadas, utilizamos o Power BI. Com ela, criamos dashboards interativos e relatórios detalhados que destacam as tendências, padrões e insights dos convênios e contratos do Governo do Estado do Ceará.

Explore nosso projeto de BI e mergulhe nas visualizações dinâmicas através do [link do nosso projeto no Power BI](www.d.com.br).


## Licença

Este projeto é disponibilizado sob a Licença MIT, que oferece ampla liberdade para uso pessoal e comercial, desde que o código original e os direitos autorais sejam mantidos. A Licença MIT é reconhecida por sua simplicidade e flexibilidade, permitindo a redistribuição e modificação do código sob os termos que garantem a atribuição adequada ao autor original. Para mais detalhes, consulte o arquivo LICENSE incluído neste repositório.
---

## Como usar este repositório

Este repositório está organizado em várias pastas que correspondem às etapas do processo de engenharia de dados e análise de dados. Para reproduzir o projeto, siga os passos contidos em cada pasta, começando pela Engenharia de Dados se for utlizar o Airflow para Configuração e Criação das Tabelas no STAGE e DW ou; pode iniciar pela pasta de Criação_DB_Stage que segue extraindo, criando as tabelas e inserindo os dados e na sequencia  a pasta com as análises do banco de dados e DataViz.


## Team:

* **Nayara Valevskii** - *Serving as Data Engineer and Documentation Specialist* - [Data Engineer]  Github (https://github.com/NayaraWakewski) | LinkedIn (https://www.linkedin.com/in/nayaraba/)
* **Isadora Xavier** - *Serving as Data Analyst and Data Visualization Creator* - [Data Analyst]
Github (https://github.com/isadoraxavier) | LinkedIn (https://www.linkedin.com/in/isadora-xavier/)

## 🎁 Words of Appreciation

* Share this project with others 📢;
* Want to learn more about the project? Get in touch for a :coffee:;

---
⌨️ with ❤️ by [Nayara Vakevskii](https://github.com/NayaraWakewski) 😊
