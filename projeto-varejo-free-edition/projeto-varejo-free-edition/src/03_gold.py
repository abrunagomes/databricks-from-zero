# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — tabelas prontas para análise
# MAGIC - `gold_faturamento_loja_dia`: faturamento, nº de vendas e ticket médio por loja e dia (sem cancelados)
# MAGIC - `gold_ranking_produtos`: produtos que mais faturam
# MAGIC - `gold_vendas_por_status`: funil de pedidos (criado, pago, enviado, cancelado)

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "varejo")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
tabela = f"{catalog}.{schema}"

# COMMAND ----------

from transformacoes import faturamento_loja_dia, ranking_produtos, vendas_por_status

vendas = spark.table(f"{tabela}.silver_vendas")
lojas = spark.table(f"{tabela}.silver_lojas")
produtos = spark.table(f"{tabela}.silver_produtos")

resultados = {
    "gold_faturamento_loja_dia": faturamento_loja_dia(vendas, lojas),
    "gold_ranking_produtos": ranking_produtos(vendas, produtos),
    "gold_vendas_por_status": vendas_por_status(vendas),
}
for nome, df in resultados.items():
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{tabela}.{nome}")

# COMMAND ----------

# Checagens finais
gold = spark.table(f"{tabela}.gold_faturamento_loja_dia")
assert gold.count() > 0, "gold_faturamento_loja_dia está vazia"
assert gold.filter("faturamento < 0 OR id_loja IS NULL").count() == 0, "Gold com valores inválidos"

display(spark.table(f"{tabela}.gold_ranking_produtos").limit(10))
