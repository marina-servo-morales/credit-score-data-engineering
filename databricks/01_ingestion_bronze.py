# Databricks notebook source
# MAGIC %md
# MAGIC # README:
# MAGIC
# MAGIC A camada raw foi armazenada no Amazon S3. Devido às limitações administrativas do Databricks Free Edition para integração direta com armazenamento externo, os arquivos foram carregados em um Volume gerenciado para processamento. Em um ambiente corporativo, a integração seria realizada por IAM Role, Storage Credential e External Location.

# COMMAND ----------

# DBTITLE 1,import
# Importa as funções do PySpark SQL como alias 'F'
# Essas funções serão usadas para manipulação de colunas e transformações de dados
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,path train e test
# Define os caminhos dos arquivos CSV armazenados no Unity Catalog Volumes
# TRAIN_PATH: conjunto de dados de treinamento com informações históricas e labels
# TEST_PATH: conjunto de dados de teste para fazer predições (sem labels)
TRAIN_PATH = "/Volumes/workspace/credit_score/files/train.csv"
TEST_PATH = "/Volumes/workspace/credit_score/files/test.csv"

# COMMAND ----------

# DBTITLE 1,df train e test
# Lê o arquivo CSV de treino, aplicando opções específicas para garantir a correta leitura dos dados
train_raw_df = (
    spark.read
    .option("header", True)           # Considera a primeira linha como cabeçalho
    .option("inferSchema", False)     # Não infere automaticamente o tipo das colunas, mantém tudo como string
    .option("multiLine", True)        # Permite que registros ocupem múltiplas linhas (útil para campos com quebras de linha)
    .option("escape", '"')            # Define o caractere de escape para aspas duplas, protegendo campos com aspas internas
    .csv(TRAIN_PATH)                  # Caminho do arquivo de treino
)

# Lê o arquivo CSV de teste, aplicando as mesmas opções para garantir consistência
test_raw_df = (
    spark.read
    .option("header", True)           # Considera a primeira linha como cabeçalho
    .option("inferSchema", False)     # Não infere automaticamente o tipo das colunas, mantém tudo como string
    .option("multiLine", True)        # Permite que registros ocupem múltiplas linhas (útil para campos com quebras de linha)
    .option("escape", '"')            # Define o caractere de escape para aspas duplas, protegendo campos com aspas internas
    .csv(TEST_PATH)                   # Caminho do arquivo de teste
)

# COMMAND ----------

# DBTITLE 1,metadados
# Cria a camada Bronze adicionando metadados de rastreabilidade aos dados brutos de treino
# Padrão Medallion Architecture: Bronze armazena dados brutos + metadados de auditoria
train_bronze_df = (
    train_raw_df
    .withColumn("_source_file", F.lit("train.csv"))              # Nome do arquivo de origem
    .withColumn("_source_system", F.lit("kaggle"))               # Sistema de origem dos dados
    .withColumn("_ingestion_timestamp", F.current_timestamp())   # Timestamp de quando os dados foram ingeridos
)

# Cria a camada Bronze para os dados de teste, aplicando os mesmos metadados
# Mantém a consistência no padrão de rastreabilidade entre treino e teste
test_bronze_df = (
    test_raw_df
    .withColumn("_source_file", F.lit("test.csv"))              # Nome do arquivo de origem
    .withColumn("_source_system", F.lit("kaggle"))               # Sistema de origem dos dados
    .withColumn("_ingestion_timestamp", F.current_timestamp())   # Timestamp de quando os dados foram ingeridos
)

# COMMAND ----------

# DBTITLE 1,display
display(train_bronze_df)

# COMMAND ----------

# DBTITLE 1,qtde de linhas, colunas e schema
print(f"Quantidade de linhas: {train_bronze_df.count()}")
print(f"Quantidade de colunas: {len(train_bronze_df.columns)}")

train_bronze_df.printSchema()

# COMMAND ----------

# DBTITLE 1,salvando como delta tables
# Salva o DataFrame da camada Bronze de treino como uma tabela Delta no Unity Catalog
# .format("delta"): define o formato Delta Lake, que permite versionamento e transações ACID
# .mode("overwrite"): sobrescreve a tabela caso ela já exista, garantindo atualização dos dados
# .option("overwriteSchema", True): permite que o schema da tabela seja atualizado conforme o DataFrame
# .saveAsTable("credit_score.bronze_train"): salva como tabela gerenciada no catálogo 'credit_score'
(
    train_bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("credit_score.bronze_train")
)

# Salva o DataFrame da camada Bronze de teste como uma tabela Delta no Unity Catalog
# Aplica as mesmas opções para garantir consistência entre treino e teste
# .saveAsTable("credit_score.bronze_test"): salva como tabela gerenciada no catálogo 'credit_score'
(
    test_bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("credit_score.bronze_test")
)