# Laboratório de Pipeline de Dados ELT com Airflow, dbt e ClickHouse

Este projeto consiste em um pipeline de dados completo no padrão ELT, construído com ferramentas open-source e totalmente conteinerizado com Docker Compose.

O objetivo é simular um ambiente de engenharia de dados real, ingerindo dados de um banco transacional (PostgreSQL), carregando-os em um Data Warehouse colunar de alta performance e preparando-os para transformação e análise com dbt.

## Arquitetura do Projeto

`PostgreSQL (Fonte)` → `Apache Airflow (Extração e Carga)` → `ClickHouse (Data Warehouse)` → `dbt (Transformação)`

As tecnologias utilizadas são:
* **Docker & Docker Compose:** Para criar um ambiente de desenvolvimento reprodutível e isolado.
* **PostgreSQL:** Simula um banco de dados transacional (OLTP) de origem.
* **ClickHouse:** Atua como nosso Data Warehouse analítico (OLAP) de alta velocidade.
* **Apache Airflow:** Orquestra todo o pipeline, agendando e monitorando a execução das tarefas de ingestão de dados.
* **dbt (Data Build Tool):** Ferramenta de transformação que será usada para criar modelos analíticos (marts) a partir dos dados brutos no ClickHouse.

Siga os passos abaixo para iniciar todo o ambiente localmente.

## Pré-requisitos
* [Docker](https://www.docker.com/products/docker-desktop/)
* Docker Compose
* Git

### 1. Clonar o Repositório
```bash
git clone [https://github.com/RangelSZ/data_engineering_lab.git](https://github.com/RangelSZ/data_engineering_lab.git)
cd data_engineering_lab
```

### 2. Configurar o Ambiente
Este projeto usa um arquivo `.env` para gerenciar as variáveis de ambiente. Crie-o a partir do exemplo:
```bash
# No Linux/macOS
cp .env.example .env

# No Windows 
copy .env.example .env
```
**Importante:** O arquivo `.env` gerado já vem com uma `AIRFLOW__CORE__FERNET_KEY` de exemplo. Para um ambiente mais seguro, gere a sua própria chave com o comando abaixo e substitua no arquivo `.env`:
```bash
docker compose run --rm airflow-webserver python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3. Iniciar os Serviços
Com o Docker em execução, suba todos os contêineres em modo "detached" (segundo plano). O comando `--build` garante que as imagens customizadas sejam construídas.

```bash
docker compose up --build -d
```
O serviço `data-loader` irá popular o PostgreSQL com os dados da Olist automaticamente. Você pode acompanhar o progresso com `docker compose logs -f data-loader`.

### 4. Configurar o Airflow
Após os serviços subirem, acesse a interface do Airflow em **`http://localhost:8080`** (login: `admin`, senha: `admin`).

Antes de rodar a DAG, você precisa criar as conexões com os bancos de dados:

* Vá em **Admin -> Connections** e crie as duas conexões abaixo:

    **Conexão PostgreSQL:**
    * **Connection Id:** `postgres_source`
    * **Connection Type:** `PostgreSQL`
    * **Host:** `postgres_db`
    * **Schema:** `airflow_db`
    * **Login:** `airflow`
    * **Password:** `airflow`
    * **Port:** `5432`

    **Conexão ClickHouse:**
    * **Connection Id:** `clickhouse_target`
    * **Connection Type:** `Generic`
    * **Host:** `clickhouse_dw`
    * **Login:** `user`
    * **Password:** `password`
    * **Port:** `9000`
    * **Extra:** `{"database": "analytics"}`

### 5. Executar o Pipeline
1.  Na página principal de DAGs, ative a DAG `olist_ingestion_postgres_to_clickhouse`.
2.  Clique no botão de "Play" para disparar uma execução manual.
3.  Acompanhe as tarefas ficando verdes. Ao final, todos os dados brutos estarão no ClickHouse, no banco de dados `raw_olist`.

Com a etapa de ingestão concluída, os próximos passos são:
- [ ] Executar os modelos dbt para criar as tabelas analíticas (`dbt run`).
- [ ] Implementar testes de qualidade de dados (`dbt test`).
- [ ] Conectar uma ferramenta de BI (Metabase ou Superset) para visualizar os resultados.
