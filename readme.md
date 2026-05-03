# 🛒 Plataforma Analítica E-commerce - Case Loovi

Este projeto consiste em uma plataforma de dados **end-to-end** de alto desempenho, integrando tecnologias modernas como **Apache Iceberg** e **DuckDB** para transformar dados brutos do ecossistema Olist em insights estratégicos.

---

## 🏗️ Arquitetura e Stack Tecnológica

O projeto utiliza a **Modern Data Stack** conteinerizada via Docker Compose:

* **Ingestão:** Python (Pandas/Boto3) consumindo arquivos CSV locais e **BrasilAPI** (Feriados Nacionais).
* **Orquestração:** **Apache Airflow** com pipeline unificada (**Master DAG**).
* **Storage:** **MinIO** (Object Storage) simulando um S3 Data Lake.
* **Table Format:** **Apache Iceberg** na camada Gold (garantindo transações ACID e Time Travel).
* **Processamento:** **DuckDB** como motor de transformação analítica (OLAP).
* **Visualização:** **Apache Superset** conectado via DuckDB In-Memory.

---

## 📂 Camadas de Dados (Arquitetura Medalhão)

* **Bronze:** Dados brutos em formato CSV (Olist) e Parquet (API), preservando o histórico original.
* **Silver:** Dados limpos, com colunas padronizadas (`snake_case`), tipagem corrigida e remoção de duplicatas.
* **Gold:** Tabelas em formato **Apache Iceberg**, modeladas em **Star Schema** para máxima performance.

---

## 📊 Documentação do Modelo Dimensional (Gold)

A camada Gold segue a metodologia de Kimball para facilitar o consumo via ferramentas de BI.

### 1. Tabela Fato: `fato_pedidos`
* **Granularidade:** Item de pedido (cada linha representa um produto dentro de uma encomenda).
* **Chave Primária (PK):** Composta (`order_id` + `order_item_id`).
* **Chaves Estrangeiras (FK):** `customer_id`, `product_id`, `date_key`.
* **Métricas principais:** `valor_produto`, `valor_frete`.

### 2. Dimensão: `dim_cliente`
* **Granularidade:** Cliente único.
* **Chave Primária (PK):** `customer_id`.
* **Atributos:** `customer_unique_id`, `customer_city`, `customer_state`.

### 3. Dimensão: `dim_produto`
* **Granularidade:** Produto único.
* **Chave Primária (PK):** `product_id`.
* **Atributos:** `product_category_name`.

### 4. Dimensão: `dim_tempo`
* **Granularidade:** Diária (Dia).
* **Chave Primária (PK):** `date_key` (Formato YYYYMMDD).
* **Atributos:** `data_completa`, `ano`, `mes`, `is_feriado` (Boolean), `tipo_dia`.

---

## 🛠️ Como Executar

1.  **Subir Infraestrutura:** No terminal, execute `docker-compose up -d`.
2.  **Rodar Pipeline:** Acesse o Airflow (`localhost:8080`) e ative a DAG `pipeline_olist_master`.
3.  **Visualizar:** Acesse o Superset (`localhost:8088`) para interagir com o dashboard.

---

## ⚖️ Decisões Técnicas e Trade-offs

* **Apache Iceberg vs Parquet Puro:** O **Apache Iceberg** foi selecionado para garantir a **atomicidade nas escritas**. Essa escolha evita que falhas durante o processo resultem em dados corrompidos ou parciais na camada Gold.
* **Ingestão via Python (Boto3) vs Airbyte:** Foi priorizado o uso de scripts Python customizados em vez do Airbyte por dois motivos:
    1.  **Eficiência de Recursos:** O Airbyte exige uma infraestrutura robusta. O uso de Python manteve o ambiente Docker leve para execução local.
    2.  **Controle Granular:** A abordagem via código permitiu um tratamento de erros específico para a BrasilAPI e a padronização imediata antes da camada Bronze.
* **DuckDB In-Memory no Superset:** Devido ao *file lock* gerado pelo DuckDB nativo durante a escrita, o motor foi configurado para ler os metadados do Iceberg diretamente no S3. Essa configuração viabiliza **consultas concorrentes** sem erros de I/O.
* **Data Quality as a Gateway:** O pipeline foi projetado para **interromper a execução (fail-fast)** caso os testes de qualidade (unicidade e volume) não sejam validados, impedindo a exposição de dados incorretos no Dashboard.

---

## 🚀 O que eu faria com mais tempo

* **Linhagem de Dados:** Implementação do **dbt** (data build tool) para gerenciar as transformações SQL e gerar documentação de linhagem automática.
* **Monitoramento de Qualidade:** Uso do **Great Expectations** para a aplicação de testes exaustivos de distribuição estatística.
* **Catálogo de Dados:** Migração do catálogo do Iceberg para o **Project Nessie** para suportar isolamento de ramos (*branching*) e governança avançada.

---

## 🔄 Fluxo de Dados (Lineage)

```mermaid
graph LR
    A[Olist CSV] --> B(Airflow Master DAG)
    H[BrasilAPI Feriados] --> B
    B --> C[(MinIO Bronze)]
    C --> D{DuckDB Engine}
    D --> E[(MinIO Silver Parquet)]
    E --> F{DuckDB + Iceberg}
    F --> G[(MinIO Gold Iceberg)]
    G --> I[Superset Dashboard]