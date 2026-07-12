# Databricks notebook source
# DBTITLE 1,import
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,silver_df
# Carrega a tabela 'workspace.credit_score.silver_customers' como um DataFrame Spark.
# Esta tabela contém os dados brutos dos clientes, que serão submetidos a validações de qualidade.
# O DataFrame resultante, 'silver_df', será utilizado nas etapas seguintes para análise, validação
# e separação dos registros válidos e rejeitados, além de gerar métricas de qualidade.
silver_df = spark.table("workspace.credit_score.silver_customers")

# COMMAND ----------

# MAGIC %skip
# MAGIC display(silver_df.limit(5))

# COMMAND ----------

# DBTITLE 1,quality_result_df
# Calcula métricas de qualidade dos dados presentes em 'silver_df'.
# As métricas incluem:
# - total_rows: número total de registros na tabela.
# - distinct_customers: número de clientes distintos (customer_id único).
# - null_customer_id: quantidade de registros com customer_id nulo.
# - invalid_or_null_age: quantidade de registros com idade nula.
# - invalid_or_null_income: quantidade de registros com renda anual nula.
# - invalid_credit_score: quantidade de registros com credit_score fora dos valores válidos ("Poor", "Standard", "Good").
quality_result_df = silver_df.agg(
    F.count("*").alias("total_rows"),
    F.countDistinct("customer_id").alias("distinct_customers"),
    F.sum(
        F.when(F.col("customer_id").isNull(), 1).otherwise(0)
    ).alias("null_customer_id"),
    F.sum(
        F.when(F.col("age").isNull(), 1).otherwise(0)
    ).alias("invalid_or_null_age"),
    F.sum(
        F.when(F.col("annual_income").isNull(), 1).otherwise(0)
    ).alias("invalid_or_null_income"),
    F.sum(
        F.when(
            ~F.col("credit_score").isin(
                "Poor",
                "Standard",
                "Good",
            ),
            1,
        ).otherwise(0)
    ).alias("invalid_credit_score"),
)

# Exibe o DataFrame com as métricas de qualidade calculadas.
# display(quality_result_df)

# COMMAND ----------

# DBTITLE 1,validated_df
# Aplica validações de qualidade nos dados do DataFrame 'silver_df'.
# Para cada registro, são criadas colunas booleanas indicando se o campo passou na validação:
# - dq_customer_id_valid: True se 'customer_id' não é nulo.
# - dq_age_valid: True se 'age' está entre 14 e 100 anos (inclusive).
# - dq_income_valid: True se 'annual_income' é maior ou igual a zero.
# - dq_credit_score_valid: True se 'credit_score' está entre os valores válidos ("Poor", "Standard", "Good").
validated_df = (
    silver_df
    .withColumn(
        "dq_customer_id_valid",
        F.col("customer_id").isNotNull(),
    )
    .withColumn(
        "dq_age_valid",
        F.col("age").between(14, 100),
    )
    .withColumn(
        "dq_income_valid",
        F.col("annual_income") >= 0,
    )
    .withColumn(
        "dq_credit_score_valid",
        F.col("credit_score").isin(
            "Poor",
            "Standard",
            "Good",
        ),
    )
)

# COMMAND ----------

# DBTITLE 1,dq_is_valid
# Adiciona a coluna 'dq_is_valid' ao DataFrame 'validated_df'.
# Esta coluna indica se o registro passou em todas as validações de qualidade:
# - dq_customer_id_valid: True se 'customer_id' não é nulo.
# - dq_age_valid: True se 'age' está entre 14 e 100 anos (inclusive).
# - dq_income_valid: True se 'annual_income' é maior ou igual a zero.
# - dq_credit_score_valid: True se 'credit_score' está entre os valores válidos ("Poor", "Standard", "Good").
# O registro é considerado válido ('dq_is_valid' = True) apenas se todas as condições acima forem satisfeitas.
validated_df = validated_df.withColumn(
    "dq_is_valid",
    (
        F.col("dq_customer_id_valid")
        & F.col("dq_age_valid")
        & F.col("dq_income_valid")
        & F.col("dq_credit_score_valid")
    ),
)

# COMMAND ----------

# DBTITLE 1,rejected_df
# Separa os registros válidos e rejeitados do DataFrame 'validated_df' com base na coluna 'dq_is_valid'.
# - valid_df: contém apenas os registros que passaram em todas as validações de qualidade ('dq_is_valid' = True).
# - rejected_df: contém os registros que falharam em pelo menos uma validação de qualidade ('dq_is_valid' = False).
valid_df = validated_df.filter(F.col("dq_is_valid"))

rejected_df = validated_df.filter(~F.col("dq_is_valid"))

# COMMAND ----------

# MAGIC %skip
# MAGIC display(rejected_df)

# COMMAND ----------

# DBTITLE 1,credit_score.valid_customers
# Salva o DataFrame 'valid_df' como uma tabela Delta chamada 'workspace.credit_score.valid_customers'.
# Esta tabela irá armazenar apenas os registros que passaram em todas as validações de qualidade,
# conforme definido pela coluna 'dq_is_valid' (True). O modo 'overwrite' garante que a tabela será
# substituída a cada execução, mantendo apenas os dados válidos mais recentes.
# Caso seja necessário atualizar o esquema da tabela, descomente a opção 'overwriteSchema'.
(
    valid_df.write
    .format("delta")  # Define o formato Delta Lake para armazenamento transacional e versionamento.
    .mode("overwrite")  # Substitui a tabela existente, garantindo dados atualizados.
    # .option("overwriteSchema", "true")  # Permite atualização do esquema, se necessário.
    .saveAsTable("workspace.credit_score.valid_customers")  # Nome da tabela onde os dados válidos serão salvos.
)

# COMMAND ----------

# DBTITLE 1,credit_score.rejected_customers
# Salva o DataFrame 'rejected_df' como uma tabela Delta chamada 'workspace.credit_score.rejected_customers'.
# Esta tabela irá armazenar apenas os registros que foram rejeitados nas validações de qualidade,
# ou seja, aqueles que falharam em pelo menos uma das seguintes condições:
# - customer_id nulo
# - idade fora do intervalo permitido (menor que 14 ou maior que 100)
# - renda anual negativa ou nula
# - credit_score fora dos valores válidos ("Poor", "Standard", "Good")
# O modo 'overwrite' garante que a tabela será substituída a cada execução, mantendo apenas os dados rejeitados mais recentes.
# Caso seja necessário atualizar o esquema da tabela, descomente a opção 'overwriteSchema'.
(
    rejected_df.write
    .format("delta")  # Define o formato Delta Lake para armazenamento transacional e versionamento.
    .mode("overwrite")  # Substitui a tabela existente, garantindo dados rejeitados atualizados.
    # .option("overwriteSchema", "true")  # Permite atualização do esquema, se necessário.
    .saveAsTable("workspace.credit_score.rejected_customers")  # Nome da tabela onde os dados rejeitados serão salvos.
)

# COMMAND ----------

# Verifica a existência de registros críticos no DataFrame 'validated_df'.
# Um registro é considerado crítico se o campo 'customer_id' estiver nulo,
# pois este campo é essencial para identificar o cliente de forma única.
# O código abaixo realiza as seguintes etapas:
# 1. Filtra os registros onde 'customer_id' é nulo.
# 2. Conta a quantidade de registros críticos encontrados.
# 3. Caso exista pelo menos um registro crítico, lança uma exceção ValueError,
#    interrompendo o processamento e informando a quantidade de registros sem 'customer_id'.

critical_error_count = (
    validated_df
    .filter(F.col("customer_id").isNull())
    .count()
)

if critical_error_count > 0:
    raise ValueError(
        f"Foram encontrados {critical_error_count} "
        "registros sem customer_id."
    )