# Databricks notebook source
# MAGIC %md
# MAGIC # TRANSFORMAÇÕES

# COMMAND ----------

# DBTITLE 1,import
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,bronze_df
bronze_df = spark.table("credit_score.bronze_train")

# COMMAND ----------

# DBTITLE 1,display bronze_df
# MAGIC %skip
# MAGIC # VALIDAÇÃO
# MAGIC display(bronze_df.limit(5))

# COMMAND ----------

# DBTITLE 1,silver_df
# Função para padronizar nomes de colunas seguindo boas práticas de nomenclatura
# Aplica as seguintes transformações:
# 1. Remove espaços em branco no início/fim (.strip())
# 2. Converte para MAIÚSCULAS (.upper()) - padrão SQL/Databricks
# 3. Substitui espaços por underscores (.replace(" ", "_"))
# 4. Substitui hífens por underscores (.replace("-", "_"))
# Resultado: nomes consistentes e fáceis de usar em queries SQL
def normalize_column_name(column_name: str) -> str:
    return (
        column_name.strip()
        .upper()
        .replace(" ", "_")
        .replace("-", "_")
    )


# Inicia a transformação Silver a partir dos dados Bronze
# Camada Silver: dados limpos, padronizados e prontos para análise
silver_df = bronze_df

# Itera por todas as colunas do DataFrame e aplica a normalização de nomes
# withColumnRenamed() renomeia cada coluna mantendo os dados intactos
# Exemplo: "Customer ID" -> "CUSTOMER_ID", "Payment-Behaviour" -> "PAYMENT_BEHAVIOUR"
for old_column in silver_df.columns:
    silver_df = silver_df.withColumnRenamed(
        old_column,
        normalize_column_name(old_column),
    )

# COMMAND ----------

# DBTITLE 1,classes
# Função para limpar strings numéricas, removendo caracteres não numéricos
# Utiliza regexp_replace para substituir qualquer caractere que não seja dígito, ponto ou hífen por vazio
# Exemplo: "1,234.56 USD" -> "1234.56"
def clean_numeric_string(column_name: str):
    return F.regexp_replace(
        F.col(column_name),
        r"[^0-9.\-]",  # Expressão regular: remove tudo exceto números, ponto e hífen
        "",
    )

# Função para limpar e formatar campos de texto
# 1. Remove espaços em branco no início/fim (F.trim)
# 2. Substitui underscores consecutivos por espaço (F.regexp_replace)
# 3. Aplica capitalização inicial em cada palavra (F.initcap)
# 4. Substitui valores inválidos ("", "_", "_______", "NA", "NM", "!@9#%8") por None
def clean_text(column_name: str):
    trimmed = F.trim(F.col(column_name))  # Remove espaços em branco

    formatted_text = F.initcap(F.regexp_replace(trimmed, "_+", " "))  # Substitui underscores por espaço e capitaliza

    return F.when(
        trimmed.isin("", "_", "_______", "NA", "NM","!@9#%8"),  # Valores considerados inválidos
        F.lit(None),  # Substitui por None
    ).otherwise(
        formatted_text  # Retorna texto limpo e formatado
    )

# COMMAND ----------

# DBTITLE 1,tratando INT, DOUBLE e STRING
# Agrupa as transformações dos campos decimais e inteiros
# Lista de colunas que devem ser tratadas como inteiros
integer_columns = [
    "AGE",  # Idade do cliente
    "NUM_BANK_ACCOUNTS",  # Número de contas bancárias
    "NUM_CREDIT_CARD",  # Número de cartões de crédito
    "INTEREST_RATE",  # Taxa de juros
    "NUM_OF_LOAN",  # Número de empréstimos
    "DELAY_FROM_DUE_DATE",  # Dias de atraso após a data de vencimento
    "NUM_OF_DELAYED_PAYMENT",  # Número de pagamentos atrasados
]

# Lista de colunas que devem ser tratadas como double (decimais)
double_columns = [
    "ANNUAL_INCOME",  # Renda anual
    "MONTHLY_INHAND_SALARY",  # Salário mensal em mãos
    "CHANGED_CREDIT_LIMIT",  # Limite de crédito alterado
    "OUTSTANDING_DEBT",  # Dívida pendente
    "CREDIT_UTILIZATION_RATIO",  # Razão de utilização de crédito
    "TOTAL_EMI_PER_MONTH",  # Valor total de EMI por mês
    "AMOUNT_INVESTED_MONTHLY",  # Valor investido mensalmente
    "NUM_CREDIT_INQUIRIES",  # Número de consultas de crédito
    "MONTHLY_BALANCE"  # Saldo mensal
]

# Lista de colunas que devem ser tratadas como string/texto
string_columns = [
    "MONTH",  # Mês
    "NAME",  # Nome do cliente
    "OCCUPATION",  # Ocupação
    "TYPE_OF_LOAN",  # Tipo de empréstimo
    "CREDIT_MIX",  # Mix de crédito
    "PAYMENT_OF_MIN_AMOUNT",  # Pagamento do valor mínimo
    "PAYMENT_BEHAVIOUR",  # Comportamento de pagamento
    "CREDIT_SCORE"  # Score de crédito
]

# Aplica limpeza e conversão para inteiro nas colunas especificadas
for col_name in integer_columns:
    if col_name in silver_df.columns:
        silver_df = silver_df.withColumn(
            col_name,
            F.when(
                clean_numeric_string(col_name) == "",  # Se valor limpo é vazio, define como None
                F.lit(None)
            ).otherwise(
                clean_numeric_string(col_name).try_cast("int") # try_cast(T.DoubleType()).try_cast(T.IntegerType())  # Converte para inteiro
            ),
        )

# Aplica limpeza e conversão para double nas colunas especificadas
for col_name in double_columns:
    if col_name in silver_df.columns:
        silver_df = silver_df.withColumn(
            col_name,
            F.when(
                clean_numeric_string(col_name) == "",  # Se valor limpo é vazio, define como None
                F.lit(None)
            ).otherwise(
                clean_numeric_string(col_name).try_cast("decimal(18,4)")
                # F.round(clean_numeric_string(col_name).try_cast(T.DoubleType()), 4)  # Converte para double e arredonda
            ),
        )

# Aplica limpeza de texto nas colunas especificadas
for col_name in string_columns:
    if col_name in silver_df.columns:
        silver_df = silver_df.withColumn(
            col_name,
            clean_text(col_name),  # Aplica função de limpeza de texto
        )

# AGE: aplica faixa válida (14 a 100)
silver_df = silver_df.withColumn(
    "AGE",
    F.when(
        F.col("AGE").between(14, 100),  # Se idade está entre 14 e 100, mantém valor
        F.col("AGE"),
    ).otherwise(F.lit(None)),  # Caso contrário, define como None
)

# NUM_BANK_ACCOUNTS: aplica faixa válida (0 a 20)
silver_df = silver_df.withColumn(
    "NUM_BANK_ACCOUNTS",
    F.when(
        F.col("NUM_BANK_ACCOUNTS").between(0, 20),  # Se número de contas está entre 0 e 20, mantém valor
        F.col("NUM_BANK_ACCOUNTS"),
    ).otherwise(F.lit(None)),  # Caso contrário, define como None
)

# NUM_CREDIT_CARD: aplica faixa válida (0 a 60)
silver_df = silver_df.withColumn(
    "NUM_CREDIT_CARD",
    F.when(
        F.col("NUM_CREDIT_CARD").between(0, 60),  # Se número de cartões está entre 0 e 60, mantém valor
        F.col("NUM_CREDIT_CARD"),
    ).otherwise(F.lit(None)),  # Caso contrário, define como None
)

# NUM_OF_LOAN: aplica faixa válida (0 a 60)
silver_df = silver_df.withColumn(
    "NUM_OF_LOAN",
    F.when(
        F.col("NUM_OF_LOAN").between(0, 60),  # Se número de empréstimos está entre 0 e 60, mantém valor
        F.col("NUM_OF_LOAN"),
    ).otherwise(F.lit(None)),  # Caso contrário, define como None
)

# COMMAND ----------

# DBTITLE 1,novas colunas
silver_df = (
    silver_df
    # Extrai o número de anos de histórico de crédito da coluna "CREDIT_HISTORY_AGE"
    # Exemplo: "2 Years 5 Months" -> 2
    # Se o valor for vazio ou "NA", define como None
    .withColumn(
        "CREDIT_HISTORY_YEARS",
        F.when(
            (F.col("CREDIT_HISTORY_AGE") == '') | (F.col("CREDIT_HISTORY_AGE") == 'NA'),
            F.lit(None)
        ).otherwise(
            F.regexp_extract(
                F.col("CREDIT_HISTORY_AGE"),
                r"(\d+)\s+Years",  # Expressão regular para capturar o número de anos
                1,
            ).cast(T.DoubleType()).cast(T.IntegerType())  # Converte para inteiro
        ),
    )
    # Extrai o número de meses de histórico de crédito da coluna "CREDIT_HISTORY_AGE"
    # Exemplo: "2 Years 5 Months" -> 5
    # Se o valor for vazio ou "NA", define como None
    .withColumn(
        "CREDIT_HISTORY_MONTHS_PART",
        F.when(
            (F.col("CREDIT_HISTORY_AGE") == '') | (F.col("CREDIT_HISTORY_AGE") == 'NA'),
            F.lit(None)
        ).otherwise(
            F.regexp_extract(
                F.col("CREDIT_HISTORY_AGE"),
                r"(\d+)\s+Months",  # Expressão regular para capturar o número de meses
                1,
            ).cast(T.DoubleType()).cast(T.IntegerType())  # Converte para inteiro
        ),
    )
    # Calcula o total de meses de histórico de crédito
    # Multiplica os anos por 12 e soma os meses
    # Se algum valor for None, considera 0
    .withColumn(
        "CREDIT_HISTORY_TOTAL_MONTHS",
        (
            F.coalesce(F.col("CREDIT_HISTORY_YEARS"), F.lit(0)) * 12
            + F.coalesce(
                F.col("CREDIT_HISTORY_MONTHS_PART"),
                F.lit(0),
            )
        ),
    )
    # Cria uma nova coluna "MONTH_NUM" convertendo o nome do mês em número
    # Usa F.lower para garantir correspondência independente de maiúsculas/minúsculas
    # Usa F.when para mapear cada mês para seu número correspondente (January=1, ..., December=12)
    .withColumn(
        "MONTH_NUM",
        F.when(F.lower(F.col("MONTH")) == "january", 1)
         .when(F.lower(F.col("MONTH")) == "february", 2)
         .when(F.lower(F.col("MONTH")) == "march", 3)
         .when(F.lower(F.col("MONTH")) == "april", 4)
         .when(F.lower(F.col("MONTH")) == "may", 5)
         .when(F.lower(F.col("MONTH")) == "june", 6)
         .when(F.lower(F.col("MONTH")) == "july", 7)
         .when(F.lower(F.col("MONTH")) == "august", 8)
         .when(F.lower(F.col("MONTH")) == "september", 9)
         .when(F.lower(F.col("MONTH")) == "october", 10)
         .when(F.lower(F.col("MONTH")) == "november", 11)
         .when(F.lower(F.col("MONTH")) == "december", 12)
         .otherwise(None)  # Caso o valor não seja um mês válido, retorna None
    )


    .withColumn(
        "LOAN_TYPES",
        F.array_distinct(
            F.transform(
                F.split(F.col("TYPE_OF_LOAN"), ","),
                lambda x: F.regexp_replace(
                    F.trim(x),
                    r"(?i)^and\s+",
                    ""
                    )
                )
            )
        )    
)

# COMMAND ----------

# DBTITLE 1,salvando silver_customers no schema
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("workspace.credit_score.silver_customers")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # VALIDAÇÕES

# COMMAND ----------

# DBTITLE 1,display silver_df
# MAGIC %skip
# MAGIC # VALIDAÇÃO
# MAGIC
# MAGIC display(silver_df.limit(5))

# COMMAND ----------

# DBTITLE 1,group by
# MAGIC %skip
# MAGIC # VALIDAÇÃO
# MAGIC
# MAGIC display(
# MAGIC     silver_df.groupBy("PAYMENT_BEHAVIOUR").count().orderBy("PAYMENT_BEHAVIOUR")
# MAGIC )

# COMMAND ----------

# DBTITLE 1,silver_temp
# MAGIC %skip
# MAGIC # VALIDAÇÃO
# MAGIC
# MAGIC silver_df.createOrReplaceTempView("silver_temp")

# COMMAND ----------

# DBTITLE 1,where CUSTOMER_ID
# MAGIC %skip
# MAGIC %sql
# MAGIC select * from silver_temp where CUSTOMER_ID = 'CUS_0x1000'

# COMMAND ----------

# MAGIC %skip
# MAGIC # VALIDAÇÃO
# MAGIC
# MAGIC display(silver_df.filter(F.col("CUSTOMER_ID") == "CUS_0x1000"))

# COMMAND ----------

# DBTITLE 1,credit_score.silver_customers
# MAGIC %skip
# MAGIC %sql
# MAGIC select * from workspace.credit_score.silver_customers where CUSTOMER_ID = 'CUS_0x1000'

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC # VALIDAÇÃO
# MAGIC
# MAGIC display(bronze_df.filter(F.col("CUSTOMER_ID") == "CUS_0x1000"))

# COMMAND ----------

# DBTITLE 1,groupBy bronze
# MAGIC %skip
# MAGIC # VALIDAÇÃO
# MAGIC
# MAGIC display(
# MAGIC     bronze_df.groupBy("Interest_Rate").count().orderBy("Interest_Rate")
# MAGIC )

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC bronze_df.filter(F.col("Name").isNull()).count()
# MAGIC
# MAGIC # bronze_df.count()

# COMMAND ----------

# DBTITLE 1,invalid_count
# MAGIC %skip
# MAGIC
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC INT_MIN = -2147483648
# MAGIC INT_MAX = 2147483647
# MAGIC
# MAGIC for col_name in integer_columns:
# MAGIC     if col_name not in silver_df.columns:
# MAGIC         continue
# MAGIC
# MAGIC     cleaned_value = clean_numeric_string(col_name)
# MAGIC
# MAGIC     numeric_value = cleaned_value.try_cast("double")
# MAGIC
# MAGIC     overflow_count = (
# MAGIC         silver_df
# MAGIC         .filter(
# MAGIC             numeric_value.isNotNull()
# MAGIC             & (
# MAGIC                 (numeric_value < F.lit(INT_MIN))
# MAGIC                 | (numeric_value > F.lit(INT_MAX))
# MAGIC             )
# MAGIC         )
# MAGIC         .count()
# MAGIC     )
# MAGIC
# MAGIC     print(
# MAGIC         f"{col_name}: {overflow_count} valores fora do limite de INTEGER"
# MAGIC     )

# COMMAND ----------

# MAGIC %skip
# MAGIC col_name = "num_of_delayed_payment"
# MAGIC
# MAGIC cleaned_value = clean_numeric_string(col_name)
# MAGIC numeric_value = cleaned_value.try_cast("double")
# MAGIC
# MAGIC display(
# MAGIC     silver_df
# MAGIC     .withColumn("_cleaned_value", cleaned_value)
# MAGIC     .withColumn("_numeric_value", numeric_value)
# MAGIC     .filter(
# MAGIC         F.col("_numeric_value").isNotNull()
# MAGIC         & (
# MAGIC             (F.col("_numeric_value") < INT_MIN)
# MAGIC             | (F.col("_numeric_value") > INT_MAX)
# MAGIC         )
# MAGIC     )
# MAGIC     .select(
# MAGIC         col_name,
# MAGIC         "_cleaned_value",
# MAGIC         "_numeric_value"
# MAGIC     )
# MAGIC )

# COMMAND ----------

# MAGIC %skip
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC INT_MIN = -2147483648
# MAGIC INT_MAX = 2147483647
# MAGIC
# MAGIC for col_name in integer_columns:
# MAGIC     if col_name not in silver_df.columns:
# MAGIC         continue
# MAGIC
# MAGIC     cleaned_value = clean_numeric_string(col_name)
# MAGIC
# MAGIC     numeric_value = cleaned_value.try_cast("double")
# MAGIC
# MAGIC     overflow_count = (
# MAGIC         silver_df
# MAGIC         .filter(
# MAGIC             numeric_value.isNotNull()
# MAGIC             & (
# MAGIC                 (numeric_value < F.lit(INT_MIN))
# MAGIC                 | (numeric_value > F.lit(INT_MAX))
# MAGIC             )
# MAGIC         )
# MAGIC         .count()
# MAGIC     )
# MAGIC
# MAGIC     print(
# MAGIC         f"{col_name}: {overflow_count} valores fora do limite de INTEGER"
# MAGIC     )

# COMMAND ----------

# MAGIC %skip
# MAGIC col_name = "num_of_delayed_payment"
# MAGIC
# MAGIC cleaned_value = clean_numeric_string(col_name)
# MAGIC numeric_value = cleaned_value.try_cast("double")
# MAGIC
# MAGIC display(
# MAGIC     silver_df
# MAGIC     .withColumn("_cleaned_value", cleaned_value)
# MAGIC     .withColumn("_numeric_value", numeric_value)
# MAGIC     .filter(
# MAGIC         F.col("_numeric_value").isNotNull()
# MAGIC         & (
# MAGIC             (F.col("_numeric_value") < INT_MIN)
# MAGIC             | (F.col("_numeric_value") > INT_MAX)
# MAGIC         )
# MAGIC     )
# MAGIC     .select(
# MAGIC         col_name,
# MAGIC         "_cleaned_value",
# MAGIC         "_numeric_value"
# MAGIC     )
# MAGIC )