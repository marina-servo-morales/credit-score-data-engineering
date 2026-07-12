# Databricks notebook source
# MAGIC %md
# MAGIC # GOLD

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,valid_df
# Carrega a tabela Delta 'workspace.credit_score.valid_customers' como um DataFrame Spark.
# Esta tabela contém os clientes válidos para análise de crédito.
# O DataFrame resultante, 'valid_df', será utilizado para processamento e enriquecimento dos dados,
# incluindo a identificação de tipos de empréstimos, criação de flags, agrupamento por idade e renda,
# e geração do DataFrame final de clientes para o GOLD layer.
valid_df = spark.table("workspace.credit_score.valid_customers")

# COMMAND ----------

# MAGIC %skip
# MAGIC distinct_loan_types_df = (
# MAGIC     valid_df
# MAGIC     .select(F.explode("LOAN_TYPES").alias("loan_type"))
# MAGIC     .filter(F.col("loan_type").isNotNull())
# MAGIC     .filter(F.trim(F.col("loan_type")) != "")
# MAGIC     .distinct()
# MAGIC     .orderBy("loan_type")
# MAGIC )
# MAGIC
# MAGIC display(distinct_loan_types_df)

# COMMAND ----------

# DBTITLE 1,df_with_flags
# Lista de tipos de empréstimos conhecidos, utilizada para criar flags binárias indicando a presença de cada tipo para o cliente.
known_loan_types = [
    "Auto Loan",
    "Credit-builder Loan",
    "Debt Consolidation Loan",
    "Home Equity Loan",
    "Mortgage Loan",
    "Not Specified",
    "Payday Loan",
    "Personal Loan",
    "Student Loan",
]

# Inicializa o DataFrame 'df_with_flags' a partir dos clientes válidos.
df_with_flags = valid_df

# Para cada tipo de empréstimo conhecido, cria uma coluna flag (1 ou 0) indicando se o cliente possui aquele tipo de empréstimo.
# O nome da coluna segue o padrão: FLAG_<TIPO_DE_EMPRESTIMO>, com letras maiúsculas e substituição de espaços e hífens por underscores.
for loan_type in known_loan_types:
    column_name = "FLAG_" + loan_type.upper().replace("-", "_").replace(" ", "_")

    # Adiciona a coluna flag ao DataFrame: 1 se o tipo de empréstimo está presente na lista 'loan_types' do cliente, 0 caso contrário.
    df_with_flags = df_with_flags.withColumn(
        column_name,
        F.when(F.array_contains(F.col("loan_types"), loan_type), 1).otherwise(0),
    )

# COMMAND ----------

# DBTITLE 1,AGE_GROUP
# Cria a coluna 'AGE_GROUP' no DataFrame 'df_with_flags', categorizando a idade dos clientes em faixas etárias.

df_with_flags = df_with_flags.withColumn(
    "AGE_GROUP",
    F.when(F.col("AGE").between(14, 17), "14-17")
    .when(F.col("AGE").between(18, 25), "18-25")
    .when(F.col("AGE").between(26, 35), "26-35")
    .when(F.col("AGE").between(36, 50), "36-50")
    .when(F.col("AGE").between(51, 60), "51-60")
    .when(F.col("AGE") > 60, "61+")
    .otherwise("Não informado"),
)

# COMMAND ----------

# DBTITLE 1,INCOME_GROUP
# Cria a coluna 'INCOME_GROUP' no DataFrame 'df_with_flags', categorizando a renda anual dos clientes em faixas de renda.

df_with_flags = df_with_flags.withColumn(
    "INCOME_GROUP",
    F.when(F.col("ANNUAL_INCOME") < 30000, "Até 30 mil")
    .when(F.col("ANNUAL_INCOME") < 60000, "30 a 60 mil")
    .when(F.col("ANNUAL_INCOME") < 100000, "60 a 100 mil")
    .when(F.col("ANNUAL_INCOME") >= 100000, "100 mil ou mais")
    .otherwise("Não informado"),
)

# COMMAND ----------

# DBTITLE 1,gold_customer_df
# Seleciona e organiza as colunas relevantes do DataFrame 'df_with_flags' para compor o DataFrame final 'gold_customer_df',
# que será utilizado para o GOLD layer. As colunas incluem informações de identificação, demográficas, financeiras,
# flags binárias para tipos de empréstimos, agrupamentos de idade e renda, além de metadados de ingestão.
gold_customer_df = df_with_flags.select(
    "ID",  # Identificador único do registro
    "CUSTOMER_ID",  # Identificador único do cliente
    "MONTH_NUM",  # Número do mês de referência
    "MONTH",  # Nome do mês de referência
    "NAME",  # Nome do cliente
    "AGE",  # Idade do cliente
    "AGE_GROUP",  # Faixa etária categorizada
    "SSN",  # Número de segurança social
    "OCCUPATION",  # Ocupação do cliente
    "CREDIT_SCORE",  # Pontuação de crédito
    "ANNUAL_INCOME",  # Renda anual
    "INCOME_GROUP",  # Faixa de renda categorizada
    "MONTHLY_INHAND_SALARY",  # Salário mensal disponível
    "NUM_BANK_ACCOUNTS",  # Número de contas bancárias
    "NUM_CREDIT_CARD",  # Número de cartões de crédito
    "INTEREST_RATE",  # Taxa de juros média dos empréstimos
    "CHANGED_CREDIT_LIMIT",  # Alteração no limite de crédito
    "NUM_CREDIT_INQUIRIES",  # Número de consultas de crédito
    "CREDIT_MIX",  # Tipo de mix de crédito
    "OUTSTANDING_DEBT",  # Dívida pendente
    "CREDIT_UTILIZATION_RATIO",  # Razão de utilização de crédito
    "CREDIT_HISTORY_TOTAL_MONTHS",  # Total de meses de histórico de crédito
    "PAYMENT_OF_MIN_AMOUNT",  # Indicador de pagamento do valor mínimo
    "TOTAL_EMI_PER_MONTH",  # Total de pagamentos mensais de empréstimos
    "AMOUNT_INVESTED_MONTHLY",  # Valor investido mensalmente
    "DELAY_FROM_DUE_DATE",  # Dias de atraso em pagamentos
    "NUM_OF_DELAYED_PAYMENT",  # Número de pagamentos atrasados
    "NUM_OF_LOAN",  # Número total de empréstimos
    "LOAN_TYPES",  # Lista de tipos de empréstimos do cliente
    # Flags binárias indicando a presença de cada tipo de empréstimo conhecido
    "FLAG_AUTO_LOAN",
    "FLAG_CREDIT_BUILDER_LOAN",
    "FLAG_DEBT_CONSOLIDATION_LOAN",
    "FLAG_HOME_EQUITY_LOAN",
    "FLAG_MORTGAGE_LOAN",
    "FLAG_PAYDAY_LOAN",
    "FLAG_PERSONAL_LOAN",
    "FLAG_STUDENT_LOAN",
    "FLAG_NOT_SPECIFIED",
    "_INGESTION_TIMESTAMP",  # Timestamp de ingestão do registro
)

# COMMAND ----------

# DBTITLE 1,credit_score.gold_customer_credit
# Salva o DataFrame 'gold_customer_df' como uma tabela Delta chamada 'workspace.credit_score.gold_customer_credit'.
# Esta operação utiliza o modo 'overwrite', substituindo completamente o conteúdo anterior da tabela.
# O formato 'delta' garante versionamento, transações ACID e performance otimizada para consultas analíticas.
# Caso seja necessário atualizar o esquema da tabela, pode-se descomentar a opção 'overwriteSchema'.
(
    gold_customer_df.write
    .format("delta")  # Define o formato Delta Lake para armazenamento da tabela.
    .mode("overwrite")  # Substitui o conteúdo existente da tabela pelo novo DataFrame.
    # .option("overwriteSchema", "true")  # (Não recomendado para camada gold) Atualiza o esquema da tabela caso tenha alterações.
    .saveAsTable("workspace.credit_score.gold_customer_credit")  # Cria ou sobrescreve a tabela Delta no workspace.
)

# COMMAND ----------

# MAGIC %md
# MAGIC # VALIDAÇÕES

# COMMAND ----------

# DBTITLE 1,filter CUSTOMER_ID
# MAGIC %skip
# MAGIC gold_customer_df.filter("CUSTOMER_ID == 'CUS_0x1000'").display()

# COMMAND ----------

# DBTITLE 1,loan_type_frequency_df
# MAGIC %skip
# MAGIC
# MAGIC loan_type_frequency_df = (
# MAGIC     valid_df
# MAGIC     .select(F.explode("loan_types").alias("loan_type"))
# MAGIC     .filter(F.col("loan_type").isNotNull())
# MAGIC     .filter(F.trim(F.col("loan_type")) != "")
# MAGIC     .groupBy("loan_type")
# MAGIC     .agg(
# MAGIC         F.count("*").alias("total_occurrences")
# MAGIC     )
# MAGIC     .orderBy(F.desc("total_occurrences"))
# MAGIC )
# MAGIC
# MAGIC display(loan_type_frequency_df)

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC select * from workspace.credit_score.gold_customer_credit where CUSTOMER_ID = 'CUS_0x1000'