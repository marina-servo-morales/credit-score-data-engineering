# ETL Automatizado de Dados Financeiros

## Marina S. Morales

Projeto final da Trilha de Engenharia de Dados do Bootcamp **[RE]Start — Data Girls**.

O projeto implementa um pipeline de dados para extração, armazenamento, transformação, validação e disponibilização de informações financeiras relacionadas à classificação de crédito de clientes.

O dataset utilizado foi o **Credit Score Classification**, disponibilizado no Kaggle.

---

## 1. Objetivo

O objetivo deste projeto é construir um pipeline de dados capaz de:

* extrair os arquivos do dataset por meio da API do Kaggle;
* armazenar os dados brutos no Amazon S3;
* carregar os arquivos em um Volume gerenciado do Databricks;
* processar os dados utilizando PySpark;
* aplicar tratamentos de qualidade e tipagem;
* organizar os dados em camadas Bronze, Silver e Gold;
* automatizar o processamento com Databricks Workflows;
* disponibilizar uma tabela final preparada para análises, dashboards e modelos de classificação de crédito.

---

## 2. Tecnologias utilizadas

* Python
* PySpark
* SQL
* Databricks Free Edition
* Delta Lake
* Unity Catalog
* Databricks Workflows
* Amazon Web Services
* Amazon S3
* AWS IAM
* Kaggle API
* KaggleHub
* GitHub

---

## 3. Fonte de dados

Foi utilizado o dataset:

```text
parisrohan/credit-score-classification
```

Os principais arquivos utilizados foram:

```text
train.csv
test.csv
```

A extração foi realizada por meio da API do Kaggle utilizando Python e a biblioteca oficial `kagglehub`.

---

## 4. Arquitetura da solução

A arquitetura implementada foi dividida em duas partes: ingestão externa e processamento no Databricks.

```text
Kaggle
   │
   │ API
   ▼
Script Python local
   │
   │ boto3
   ▼
Amazon S3
Camada raw
   │
   │ carga manual
   ▼
Databricks Managed Volume
   │
   ▼
Tabela Bronze
   │
   ▼
Tabela Silver
   │
   ▼
Validações de qualidade
   │
   ▼
Tabela Gold
```

### 4.1 Ingestão externa

A primeira parte do pipeline foi executada por scripts Python locais:

```text
Kaggle API
    ↓
Download dos arquivos
    ↓
Armazenamento local temporário
    ↓
Upload para o Amazon S3
```

A extração do Kaggle e o upload dos arquivos para o Amazon S3 foram realizados de forma programática, sem download manual pela interface do Kaggle.

### 4.2 Ingestão no Databricks

A camada raw foi armazenada no Amazon S3.

Devido às limitações administrativas do Databricks Free Edition para configuração de armazenamento externo, os arquivos foram carregados manualmente em um **Volume gerenciado pelo Unity Catalog**.

Os arquivos foram acessados pelos notebooks por meio dos caminhos:

```python
TRAIN_PATH = "/Volumes/workspace/credit_score/files/train.csv"
TEST_PATH = "/Volumes/workspace/credit_score/files/test.csv"
```

Portanto, a fonte direta da tabela Bronze dentro do Databricks foi o Managed Volume.

Em um ambiente corporativo, a etapa manual entre S3 e Databricks seria substituída por uma integração direta utilizando:

```text
IAM Role
    ↓
Storage Credential
    ↓
External Location
    ↓
External Volume ou tabela externa
```

Essa arquitetura permitiria que o Databricks lesse os arquivos diretamente do Amazon S3, sem necessidade de upload manual para o Volume.

---

## 5. Camadas do pipeline

O pipeline foi organizado seguindo os princípios da arquitetura Medallion.

### 5.1 Raw

A camada raw contém os arquivos originais extraídos do Kaggle.

Localização no Amazon S3:

```text
s3://<nome-do-bucket>/credit-score/raw/train.csv
s3://<nome-do-bucket>/credit-score/raw/test.csv
```

Os arquivos são armazenados sem alterações, preservando os dados recebidos da fonte.

### 5.2 Bronze

A camada Bronze contém os dados carregados a partir dos arquivos CSV presentes no Volume gerenciado do Databricks.

Nessa camada, os dados são mantidos próximos ao formato original.

Também foi adicionada a coluna de auditoria:

```text
_INGESTION_TIMESTAMP
```

Essa coluna registra o momento em que os dados foram processados pelo pipeline.

### 5.3 Silver

Na camada Silver foram realizadas as principais transformações, padronizações e validações dos dados.

As transformações incluíram:

* padronização dos nomes das colunas;
* conversão de tipos;
* tratamento de valores vazios;
* tratamento de valores sentinela;
* limpeza de caracteres inválidos;
* conversão de colunas financeiras para decimal;
* conversão de colunas quantitativas para inteiro;
* tratamento da idade;
* criação de faixas etárias;
* criação de faixas de renda;
* conversão do mês para representação numérica;
* conversão do histórico de crédito para quantidade total de meses;
* tratamento da coluna `TYPE_OF_LOAN`;
* criação de flags por tipo de empréstimo;
* padronização da classificação de crédito.

### 5.4 Quality

A etapa de qualidade executa verificações antes da disponibilização da camada Gold.

Entre as validações realizadas estão:

* existência das colunas obrigatórias;
* validação de tabela vazia;
* registros sem identificador de cliente;
* valores numéricos inválidos;
* idade fora do intervalo esperado;
* valores financeiros malformados;
* valores sentinela;
* classificações de crédito inválidas;
* consistência dos tipos de empréstimo.

Caso uma validação crítica falhe, o notebook gera uma exceção e impede a execução da task seguinte.

### 5.5 Gold

A camada Gold contém os dados tratados e estruturados para consumo analítico.

Ela pode ser utilizada por:

* equipes de Analytics;
* dashboards;
* Power BI;
* estudos de risco de crédito;
* modelos de classificação;
* análises de perfil financeiro;
* análises de produtos de crédito.

---

## 6. Extração por API do Kaggle

A extração foi implementada por meio de um script Python.

Fluxo:

```text
API do Kaggle
    ↓
kagglehub.dataset_download()
    ↓
Cache local do KaggleHub
    ↓
Cópia dos arquivos para data/raw
```

Exemplo simplificado:

```python
from pathlib import Path
import shutil

import kagglehub


DATASET_HANDLE = "parisrohan/credit-score-classification"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIRECTORY = PROJECT_ROOT / "data" / "raw"


def extract_dataset() -> None:
    RAW_DIRECTORY.mkdir(parents=True, exist_ok=True)

    downloaded_directory = Path(
        kagglehub.dataset_download(DATASET_HANDLE)
    )

    for source_file in downloaded_directory.rglob("*"):
        if source_file.is_file():
            shutil.copy2(
                source_file,
                RAW_DIRECTORY / source_file.name,
            )


if __name__ == "__main__":
    extract_dataset()
```

As credenciais do Kaggle não são armazenadas no código-fonte e não são enviadas ao GitHub.

---

## 7. Upload para o Amazon S3

Depois da extração, os arquivos são enviados para o Amazon S3 utilizando Python e a biblioteca `boto3`.

Fluxo:

```text
data/raw/train.csv
data/raw/test.csv
        ↓
      boto3
        ↓
Amazon S3
```

Estrutura utilizada no bucket:

```text
credit-score/
└── raw/
    ├── train.csv
    └── test.csv
```

O usuário IAM utilizado pelo script possui acesso restrito ao bucket e ao prefixo do projeto.

As credenciais da AWS são armazenadas em variáveis de ambiente e não são incluídas no repositório.

---

## 8. Tratamento da coluna TYPE_OF_LOAN

A coluna `TYPE_OF_LOAN` contém múltiplos tipos de empréstimos separados por vírgula.

Exemplo:

```text
Auto Loan, Not Specified, Student Loan, Home Equity Loan,
Payday Loan, Payday Loan, And Credit-builder Loan
```

O tratamento realizado foi:

1. separação dos valores por vírgula;
2. remoção de espaços;
3. remoção do prefixo `And`;
4. remoção de duplicidades;
5. criação de um array com os tipos de empréstimos;
6. criação de flags individuais.

Após o tratamento, o exemplo passa a ser representado como:

```text
[
  "Auto Loan",
  "Not Specified",
  "Student Loan",
  "Home Equity Loan",
  "Payday Loan",
  "Credit-builder Loan"
]
```

Os tipos distintos encontrados foram:

```text
Auto Loan
Credit-builder Loan
Debt Consolidation Loan
Home Equity Loan
Mortgage Loan
Not Specified
Payday Loan
Personal Loan
Student Loan
```

Foram criadas as seguintes flags para facilitar análises de BI:

```text
FLAG_AUTO_LOAN
FLAG_CREDIT_BUILDER_LOAN
FLAG_DEBT_CONSOLIDATION_LOAN
FLAG_HOME_EQUITY_LOAN
FLAG_MORTGAGE_LOAN
FLAG_PAYDAY_LOAN
FLAG_PERSONAL_LOAN
FLAG_STUDENT_LOAN
FLAG_NOT_SPECIFIED
```

Cada flag recebe:

```text
1 — cliente possui o tipo de empréstimo
0 — cliente não possui o tipo de empréstimo
```

A coluna original também foi transformada em:

```text
LOAN_TYPES array<string>
```

Essa estrutura permite análises tanto pelo array quanto pelas colunas indicadoras.

---

## 9. Tratamento de valores financeiros

As colunas financeiras foram convertidas para:

```text
decimal(18,4)
```

Esse tipo foi escolhido para:

* preservar precisão;
* evitar arredondamento excessivo;
* representar adequadamente valores monetários;
* evitar os problemas de imprecisão do tipo `double`.

Exemplo:

```text
252.9247932365056
```

passa a ser representado como:

```text
252.9248
```

Entre as colunas convertidas estão:

```text
ANNUAL_INCOME
MONTHLY_INHAND_SALARY
CHANGED_CREDIT_LIMIT
NUM_CREDIT_INQUIRIES
OUTSTANDING_DEBT
CREDIT_UTILIZATION_RATIO
TOTAL_EMI_PER_MONTH
AMOUNT_INVESTED_MONTHLY
```

Durante a exploração dos dados, foi identificado um valor sentinela inválido na coluna de saldo mensal:

```text
__-333333333333333333333333333__
```

Esse valor ultrapassa os limites esperados para um dado financeiro e foi tratado como inválido.

A conversão utilizou `try_cast`, permitindo que valores malformados fossem convertidos para `NULL` em vez de interromper todo o pipeline.

---

## 10. Schema final

O schema final da tabela processada ficou definido da seguinte forma:

| Coluna                       | Tipo          |
| ---------------------------- | ------------- |
| ID                           | string        |
| CUSTOMER_ID                  | string        |
| MONTH_NUM                    | int           |
| MONTH                        | string        |
| NAME                         | string        |
| AGE                          | int           |
| AGE_GROUP                    | string        |
| SSN                          | string        |
| OCCUPATION                   | string        |
| CREDIT_SCORE                 | string        |
| ANNUAL_INCOME                | decimal(18,4) |
| INCOME_GROUP                 | string        |
| MONTHLY_INHAND_SALARY        | decimal(18,4) |
| NUM_BANK_ACCOUNTS            | int           |
| NUM_CREDIT_CARD              | int           |
| INTEREST_RATE                | int           |
| CHANGED_CREDIT_LIMIT         | decimal(18,4) |
| NUM_CREDIT_INQUIRIES         | decimal(18,4) |
| CREDIT_MIX                   | string        |
| OUTSTANDING_DEBT             | decimal(18,4) |
| CREDIT_UTILIZATION_RATIO     | decimal(18,4) |
| CREDIT_HISTORY_TOTAL_MONTHS  | int           |
| PAYMENT_OF_MIN_AMOUNT        | string        |
| TOTAL_EMI_PER_MONTH          | decimal(18,4) |
| AMOUNT_INVESTED_MONTHLY      | decimal(18,4) |
| DELAY_FROM_DUE_DATE          | int           |
| NUM_OF_DELAYED_PAYMENT       | int           |
| NUM_OF_LOAN                  | int           |
| LOAN_TYPES                   | array<string> |
| FLAG_AUTO_LOAN               | int           |
| FLAG_CREDIT_BUILDER_LOAN     | int           |
| FLAG_DEBT_CONSOLIDATION_LOAN | int           |
| FLAG_HOME_EQUITY_LOAN        | int           |
| FLAG_MORTGAGE_LOAN           | int           |
| FLAG_PAYDAY_LOAN             | int           |
| FLAG_PERSONAL_LOAN           | int           |
| FLAG_STUDENT_LOAN            | int           |
| FLAG_NOT_SPECIFIED           | int           |
| _INGESTION_TIMESTAMP         | timestamp     |

---

## 11. Automação com Databricks Workflows

O processamento foi automatizado utilizando um Databricks Job.

Nome do Job:

```text
credit-score-etl-job
```

O Job contém quatro tasks do tipo Notebook, executadas sequencialmente.

```text
ingestion_bronze
        ↓
transformation_silver
        ↓
quality_checks
        ↓
create_gold
```

### Task 1 — ingestion_bronze

Responsável por:

* ler os arquivos do Volume;
* adicionar metadados de ingestão;
* criar as tabelas Bronze.

### Task 2 — transformation_silver

Responsável por:

* limpar os dados;
* converter tipos;
* tratar valores inválidos;
* criar colunas derivadas;
* tratar os tipos de empréstimo;
* criar a tabela Silver.

### Task 3 — quality_checks

Responsável por:

* executar regras de qualidade;
* verificar colunas obrigatórias;
* validar registros;
* identificar dados inválidos;
* interromper o pipeline em caso de erro crítico.

### Task 4 — create_gold

Responsável por:

* consumir os dados aprovados;
* selecionar as colunas finais;
* criar a camada Gold;
* disponibilizar os dados para consumo analítico.

Todas as quatro tasks foram executadas com sucesso no Databricks Workflows.

O Job garante que uma task seja iniciada somente depois da conclusão bem-sucedida da task anterior.

Caso a task de qualidade falhe, a criação da camada Gold não é executada.

---

## 12. Estratégia de carga

Como o dataset utilizado é disponibilizado como carga completa, foi adotada a estratégia:

```text
Full refresh
```

As tabelas são gravadas utilizando:

```python
.mode("overwrite")
```

Essa escolha garante idempotência: o pipeline pode ser executado novamente sem duplicar os registros existentes.

Em um cenário produtivo com atualizações incrementais, poderia ser utilizada uma operação `MERGE INTO`, considerando uma chave de negócio como:

```text
CUSTOMER_ID + MONTH
```

Exemplo conceitual:

```sql
MERGE INTO credit_score.silver_customers AS target
USING credit_score.silver_staging AS source
ON target.CUSTOMER_ID = source.CUSTOMER_ID
AND target.MONTH = source.MONTH

WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

---

## 13. Organização do repositório

```text
credit-score-data-engineering/
│
├── data/
│   ├── raw/
│
├── databricks/
│   ├── 01_ingestion_bronze.py
│   ├── 02_transformation_silver.py
│   ├── 03_quality_checks.py
│   └── 04_gold.py
│
├── docs/
│   ├── architecture.png
│   └── screenshots/
│
├── src/
│   ├── extract_kaggle.py
│   ├── upload_s3.py
│   └── main.py
│
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

Os arquivos de dados não são enviados ao GitHub.

---

## 14. Segurança

As seguintes informações não são versionadas:

* token da API do Kaggle;
* Access Key ID da AWS;
* Secret Access Key da AWS;
* arquivo `.env`;
* arquivos de dados;
* credenciais locais;
* configurações pessoais do Databricks.

O arquivo `.gitignore` inclui:

```gitignore
.env
.venv/
kaggle.json
access_token
.kaggle/
data/raw/*
data/processed/*
__pycache__/
*.log
```

As credenciais da AWS são utilizadas por variáveis de ambiente.

O bucket S3 permanece privado e com bloqueio de acesso público ativado.

---

## 15. Como executar o projeto

### 15.1 Criar o ambiente virtual

No Git Bash:

```bash
python -m venv .venv
source .venv/Scripts/activate
```

### 15.2 Instalar as dependências

```bash
pip install -r requirements.txt
```

### 15.3 Configurar as credenciais

As credenciais do Kaggle devem ser configuradas no ambiente local.

As credenciais da AWS devem ser definidas no arquivo `.env`:

```env
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=
```

### 15.4 Executar a extração

```bash
python src/extract_kaggle.py
```

### 15.5 Executar o upload para o S3

```bash
python src/upload_s3.py
```

### 15.6 Carregar os arquivos no Databricks

No Databricks Free Edition:

1. acessar o Catalog Explorer;
2. abrir o schema `credit_score`;
3. abrir o Volume `files`;
4. fazer upload de `train.csv` e `test.csv`;
5. confirmar os caminhos dos arquivos.

```text
/Volumes/workspace/credit_score/files/train.csv
/Volumes/workspace/credit_score/files/test.csv
```

### 15.7 Executar o Job

No menu do Databricks:

```text
Jobs & Pipelines
→ credit-score-etl-job
→ Run now
```

O Job executará:

```text
Bronze → Silver → Quality → Gold
```

---

## 16. Perguntas norteadoras

### Como garantir que os dados estejam atualizados e prontos para o negócio?

O pipeline organiza as etapas de extração, armazenamento, transformação e disponibilização.

A execução por meio de um Databricks Job garante que as tarefas sejam processadas em sequência e que falhas sejam registradas.

A coluna `_INGESTION_TIMESTAMP` permite identificar quando cada carga foi processada.

Em um ambiente produtivo, o Job poderia ser agendado para executar diariamente ou conforme a frequência de atualização da fonte.

### Quais validações devem ser realizadas?

Entre as validações recomendadas estão:

* presença de identificadores obrigatórios;
* validação dos tipos das colunas;
* detecção de valores nulos;
* detecção de valores sentinela;
* validação de intervalos de idade;
* validação de valores financeiros;
* validação das categorias de score;
* identificação de duplicidades;
* validação da estrutura da tabela;
* verificação de tabela vazia.

### Como evitar registros duplicados?

Neste projeto foi utilizada uma estratégia de carga completa com sobrescrita das tabelas.

A operação `overwrite` torna o processamento idempotente e evita duplicações entre execuções.

Para uma ingestão incremental, a chave composta por `CUSTOMER_ID` e `MONTH` poderia ser utilizada em uma operação `MERGE`.

### Como organizar os dados para análises?

Os dados foram organizados em camadas:

```text
Raw → Bronze → Silver → Gold
```

A camada Gold apresenta:

* tipos corrigidos;
* dados padronizados;
* colunas derivadas;
* faixas de renda;
* faixas etárias;
* tipos de empréstimos estruturados;
* flags analíticas;
* metadados de ingestão.

Essa estrutura facilita a conexão com ferramentas de BI e a utilização em modelos preditivos.

---

## 17. Limitações

A principal limitação do projeto foi a ausência de integração direta entre o Databricks Free Edition e o bucket S3 criado na conta AWS.

Por esse motivo, os arquivos foram carregados manualmente do S3 para um Managed Volume.

A automação implementada no Databricks começa no Volume:

```text
Managed Volume
    ↓
Bronze
    ↓
Silver
    ↓
Quality
    ↓
Gold
```

Apesar da etapa manual, a extração do Kaggle e o armazenamento no Amazon S3 foram realizados de forma automatizada por API e scripts Python.

---

## 18. Melhorias futuras

As próximas evoluções possíveis incluem:

* conexão direta do Databricks com o Amazon S3;
* utilização de IAM Role;
* criação de Storage Credential;
* criação de External Location;
* utilização de External Volume;
* eliminação da etapa manual de upload;
* criação de carga incremental;
* utilização de `MERGE INTO`;
* criação de métricas de qualidade;
* monitoramento e alertas;
* integração com Power BI;
* criação de testes automatizados;
* deploy por Databricks Asset Bundles;
* integração contínua pelo GitHub Actions;
* utilização de Airflow para orquestrar também a extração externa.

---

## 19. Resultados

O projeto resultou em:

* extração do dataset pela API do Kaggle;
* armazenamento dos arquivos raw no Amazon S3;
* organização dos arquivos em um Volume do Unity Catalog;
* criação das camadas Bronze, Silver e Gold;
* tratamento dos dados utilizando PySpark;
* criação de regras de qualidade;
* tratamento dos tipos de empréstimos;
* criação de flags analíticas;
* conversão e padronização dos tipos;
* execução bem-sucedida de um Databricks Job com quatro tasks sequenciais;
* disponibilização de uma tabela final pronta para consumo analítico.

---

## 20. Conclusão

O projeto demonstra a construção de um pipeline completo de Engenharia de Dados, cobrindo:

```text
Extração
Armazenamento
Ingestão
Transformação
Qualidade
Orquestração
Documentação
Versionamento
```

Mesmo utilizando ambientes gratuitos, foi possível simular uma arquitetura próxima de um cenário corporativo.

A solução também diferencia claramente as limitações do ambiente utilizado e a arquitetura que seria aplicada em produção.

