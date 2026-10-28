# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Cell 2
# MAGIC %md
# MAGIC ###Bronze - ingestão dos CSVs exportados do banco
# MAGIC Lê os arquivos CSV do Volume `origem_csv` (exportados das tabelas do banco) e grava tabelas Delta, acrescentando metadados de ingestão.
# MAGIC
# MAGIC | Tabela | Estratégia |
# MAGIC |---|---|
# MAGIC | `lojas`, `produtos`, `clientes` | Carga completa (sobrescreve a tabela) |
# MAGIC | `vendas` | **Incremental**: lê só linhas novas ou alteradas desde a última ingestão (coluna `atualizado_em`) e acrescenta |
# MAGIC

# COMMAND ----------

# MAGIC %run ./utils/imports

# COMMAND ----------

# MAGIC %run ./utils/auto_loader_bronze

# COMMAND ----------

dbutils.widgets.text("catalog", "user_group")
dbutils.widgets.text("schema", "project_from_zero")
dbutils.widgets.text("volume", "ingestion_files")
#dbutils.widgets.text("arquivo_vendas", "vendas.csv")  # nome do CSV de vendas dentro do Volume

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
#arquivo_vendas = dbutils.widgets.get("arquivo_vendas")

tabela = f"{catalog}.{schema}"
pasta = f"/Volumes/{catalog}/{schema}/{volume}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {tabela}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {tabela}.{volume}")

# COMMAND ----------

# Confere se os arquivos estão no Volume antes de começar
arquivos = {
    "lojas": "lojas.csv",
    "produtos": "produtos.csv",
    "clientes": "clientes.csv",
    "vendas": "vendas_lote0.csv",
}
existentes = [f.name for f in dbutils.fs.ls(pasta)]
faltando = [nome for nome in arquivos.values() if nome not in existentes]
if faltando:
    raise FileNotFoundError(f"Envie estes arquivos para {pasta}: {faltando}")


# COMMAND ----------

from pyspark.sql import functions as F

from mock_dados import SCHEMAS_SPARK
from transformacoes import tipar_csv


def ler_csv(nome, desde=None):
    """Lê o CSV como texto e converte para os tipos da tabela. Se `desde` for informado,
    devolve só as linhas alteradas a partir dessa data (carga incremental)."""
    bruto = spark.read.option("header", "true").csv(f"{pasta}/{arquivos[nome]}")
    df = tipar_csv(bruto, SCHEMAS_SPARK[nome])
    if desde:
        df = df.filter(F.col("atualizado_em") >= desde)
    return df


def gravar(df, nome, modo):
    df = df.withColumn("_ingestao_ts", F.current_timestamp()).withColumn("_origem", F.lit("csv"))
    escritor = df.write.mode(modo)
    if modo == "overwrite":
        escritor = escritor.option("overwriteSchema", "true")
    escritor.saveAsTable(f"{tabela}.bronze_{nome}")


# COMMAND ----------


# Cadastros: carga completa
for nome in ["lojas", "produtos", "clientes"]:
    gravar(ler_csv(nome), nome, "overwrite")
    print(f"bronze_{nome}: carga completa")


# COMMAND ----------

# Vendas: incremental com marca d'água (a data da última atualização já ingerida)
bronze_vendas = f"{tabela}.bronze_vendas"
marca = None
if spark.catalog.tableExists(bronze_vendas):
    marca = spark.sql(f"SELECT MAX(atualizado_em) FROM {bronze_vendas}").first()[0]

novas = ler_csv("vendas", desde=marca)
print(f"Marca d'água: {marca} · linhas lidas do CSV: {novas.count()}")
gravar(novas, "vendas", "append")
# Usamos >= (e não >) para nunca perder linhas na borda; os repetidos são removidos na Silver.

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS linhas, MAX(atualizado_em) AS ultima_atualizacao FROM {bronze_vendas}"))