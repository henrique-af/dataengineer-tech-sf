## **Data Engineer - Teste Técnico**

Este repositório contém uma simulação de um pipeline de dados utilizando Google Cloud Platform (GCP) e Apache Airflow, conforme solicitado no teste técnico. O objetivo é demonstrar o conhecimento na construção de pipelines de dados que integram múltiplas etapas, desde a extração até a entrega dos dados processados.

### **Arquitetura Implementada**

A arquitetura proposta e a implementação são descritas abaixo:

**Arquitetura Proposta:**
- **Google Cloud Storage (GCS):** Repositório para dados brutos e processados.
- **Pub/Sub:** Serviço para ingestão e transmissão de dados em tempo real.
- **Airflow (Google Cloud Composer):** Orquestrador para o pipeline de dados.
- **Dataflow:** Processamento e transformação de dados.
- **Python:** Linguagem para script e automação.
- **BigQuery:** Armazenamento e análise de dados.
- **Looker:** Visualização e relatórios.

**Arquitetura Implementada:**
- **Airflow em Docker:** Um container Docker com Apache Airflow foi configurado e está funcional. 
- **Pasta Local Simulando GCS:** Utilizamos uma pasta local dentro do ambiente Docker para simular o Google Cloud Storage (GCS). 
- **DAG de ETL:** Dentro do Airflow, há uma DAG que realiza um processo de ETL conforme descrito neste README. 

Embora a implementação não inclua todos os componentes da arquitetura proposta, o ETL completo está operacional e demonstrado no ambiente configurado.


### **Subindo o Ambiente**

Para configurar e iniciar o ambiente Docker com Apache Airflow, siga os passos abaixo:


1. **Subir o ambiente Docker:**
   Na raiz do projeto, execute:
   ```bash
   docker-compose up -d
   ```

2. **Copiar arquivos para o ambiente Docker:**
   Após iniciar o Docker, execute o seguinte script Python na raiz do projeto para copiar os arquivos necessários para o ambiente Docker:
   ```bash
   python project/copy_files_package/copy_files.py
   ```
   Este script copiará os arquivos para o ambiente Docker e os tornará disponíveis para o Airflow.

3. **Abrir o dashboard do Airflow:**
   Após iniciar o Docker e copiar os arquivos, abra seu navegador e acesse o dashboard do Airflow na seguinte URL:
   ```
   http://localhost:8080
   ```
   Use as seguintes credenciais para fazer login:
   - **Usuário:** airflow
   - **Senha:** airflow

### **Explicação das DAGs**

#### **DAG_001 - `dag_001_capture_and_generate_report`**

A DAG foi projetada para realizar o seguinte fluxo de ETL:

1. **Verificar Arquivos:**
   - **Tarefa:** `check_files`
   - **Descrição:** Usa a função `check_files` para verificar a presença de arquivos com a data atual na pasta de input. Se um arquivo com a data de hoje for encontrado, a execução da DAG continua.

2. **Descompactar Arquivos:**
   - **Tarefa:** `unzip_file`
   - **Descrição:** Utiliza a função `unzip_file` para descompactar arquivos ZIP encontrados na pasta de input para a pasta de processamento.

3. **Processar Arquivos CSV:**
   - **Tarefa:** `process_csv`
   - **Descrição:** A função `process_csv` lê os arquivos CSV na pasta de processamento, gera uma query SQL para inserir ou atualizar dados em uma tabela de trabalho e salva a query em um arquivo SQL.

4. **Atualizar o Banco de Dados:**
   - **Tarefa:** `update_database`
   - **Descrição:** A função `update_database` executa a query gerada no banco de dados PostgreSQL para atualizar a tabela de trabalho com os dados processados.

5. **Gerar Relatório:**
   - **Tarefa:** `generate_report`
   - **Descrição:** A função `generate_report` cria um relatório em CSV com os pedidos que foram entregues e o salva na pasta de output.

6. **Limpar Arquivos:**
   - **Tarefa:** `cleanup_files`
   - **Descrição:** A função `cleanup_files` remove arquivos antigos e processados das pastas de input e processamento.

   **Cronograma:** A DAG executa a cada minuto entre 4h e 7h, de segunda a sexta-feira.

#### **DAG Conceito - `dag_conceito`**

A DAG simula um fluxo de dados em um ambiente de GCP com as seguintes etapas:

1. **Mover Arquivos no GCS:**
   - **Tarefa:** `move_gcs_files`
   - **Descrição:** Utiliza o `GCSToGCSOperator` para mover arquivos do bucket de input para o bucket de processamento no Google Cloud Storage.

2. **Iniciar Job de Dataflow:**
   - **Tarefa:** `start_dataflow`
   - **Descrição:** Usa o `DataflowStartFlexTemplateOperator` para iniciar um job de Dataflow com um template para processar o arquivo CSV.

3. **Carregar Dados no BigQuery:**
   - **Tarefa:** `load_to_bigquery`
   - **Descrição:** Utiliza o `GCSToBigQueryOperator` para carregar o arquivo processado no BigQuery, substituindo a tabela existente.

4. **Limpar Arquivos no GCS:**
   - **Tarefa:** `cleanup_files`
   - **Descrição:** A função `cleanup_gcs_files` remove arquivos antigos da pasta de processamento no GCS.

   **Cronograma:** A DAG executa diariamente às 7h, de segunda a sexta-feira.

### **Comparação com a Arquitetura Proposta**

1. **Landing Zone**
   - **Arquitetura Proposta:** Armazena dados brutos no GCS e utiliza Pub/Sub para ingestão em tempo real.
   - `dag_001`: Processa arquivos ZIP na pasta de input local, descompacta e prepara os dados, alinhando-se com o conceito de pré-processamento na Landing Zone.

2. **Orchestration Layer**
   - **Arquitetura Proposta:** O Airflow orquestra os workflows e utiliza Pub/Sub para coordenar processos.
   - `dag_001` e `dag_conceito`: Ambas utilizam o Airflow para coordenar tarefas e workflows, refletindo o papel de orquestração descrito.

3. **Transformation Layer**
   - **Arquitetura Proposta:** Dataflow processa e transforma dados brutos.
   - `dag_001`: Orquestra a descompactação e preparação de dados para transformação. 
    -  `dag_conceito`: Integra Dataflow para transformar dados, alinhando-se com a transformação de dados.

4. **Staging Area**
   - **Arquitetura Proposta:** Armazena dados temporariamente no GCS antes da carga final.
   - `dag_001`: Limpa e move arquivos após processamento, similar ao armazenamento temporário na Staging Area.

5. **Data Warehouse**
   - **Arquitetura Proposta:** BigQuery armazena dados transformados para análises rápidas.
   - `dag_001`: Embora não interaja diretamente com o BigQuery, prepara dados para uma carga futura em um Data Warehouse.

6. **Analytics Layer**
   - **Arquitetura Proposta:** Looker cria visualizações e relatórios baseados no Data Warehouse.
   - `dag_conceito`: Foca na carga de dados no BigQuery, preparando-os para análises e relatórios, conforme descrito na Analytics Layer.

### **Observações**

- As DAGs foram projetadas para imitar o fluxo de dados em ambientes de produção, desde a chegada dos dados até o processamento e armazenamento final.
- A `dag_conceito` é uma simulação que não está completamente funcional, mas reproduz o conceito de integração com o Google Cloud.