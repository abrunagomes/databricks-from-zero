# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ### Silver - limpeza e qualidade
# MAGIC - Padroniza textos (e-mail, cidade, status)
# MAGIC - Fica só com a **versão mais recente** de cada venda
# MAGIC - Trata desconto não informado e calcula `valor_total`
# MAGIC - Registros inválidos vão para `silver_vendas_rejeitadas` (com o motivo de rejeição)

# COMMAND ----------

dbutils.widgets.text("catalog", "user_group")
dbutils.widgets.text("schema", "project_from_zero")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
tabela = f"{catalog}.{schema}"

# COMMAND ----------

# MAGIC %run ./utils/imports

# COMMAND ----------

def salvar(df, nome):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{tabela}.{nome}")


salvar(limpar_clientes(spark.table(f"{tabela}.bronze_clientes")), "silver_clientes")
salvar(spark.table(f"{tabela}.bronze_lojas").dropDuplicates(["id_loja"]).drop("_ingestao_ts", "_origem"), "silver_lojas")
salvar(spark.table(f"{tabela}.bronze_produtos").dropDuplicates(["id_produto"]).drop("_ingestao_ts", "_origem"), "silver_produtos")

bronze_vendas = spark.table(f"{tabela}.bronze_vendas")
validas, rejeitadas = limpar_vendas(bronze_vendas)
salvar(validas, "silver_vendas")
salvar(rejeitadas, "silver_vendas_rejeitadas")

# COMMAND ----------

# Regra de qualidade: o job falha se mais de 10% das vendas forem rejeitadas

total = spark.table(f"{tabela}.silver_vendas").count() + spark.table(f"{tabela}.silver_vendas_rejeitadas").count()

n_rejeitadas = spark.table(f"{tabela}.silver_vendas_rejeitadas").count()
taxa = verificar_taxa_rejeicao(total, n_rejeitadas, limite=0.10)

print(f"Vendas: {total} · rejeitadas: {n_rejeitadas} ({taxa:.1%})")



# COMMAND ----------

display(spark.table(f"{tabela}.silver_vendas_rejeitadas").groupBy("motivo_rejeicao").count())