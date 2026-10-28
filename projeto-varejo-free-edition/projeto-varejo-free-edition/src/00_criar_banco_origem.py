# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Criar o banco de dados de origem (mock)
# MAGIC Cria um **banco SQLite de verdade** (tabelas `lojas`, `produtos`, `clientes`, `vendas`) e o guarda em um
# MAGIC Volume do Unity Catalog. Ele faz o papel do banco de produção da empresa: é de onde o pipeline vai ingerir.
# MAGIC
# MAGIC - `lote = 0`: carga inicial (cadastros + 400 vendas do dia 01/10)
# MAGIC - `lote = 1`: novas vendas do dia 02/10 **e** mudanças de status em vendas antigas
# MAGIC - `lote = 2`, `3`...: mesma ideia, um dia por vez
# MAGIC
# MAGIC Pode rodar várias vezes: o resultado não duplica.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "varejo")
dbutils.widgets.text("lote", "0")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
lote = int(dbutils.widgets.get("lote"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.origem")

# COMMAND ----------

import os
import shutil
import sqlite3

from mock_dados import aplicar_lote_sqlite

banco_volume = f"/Volumes/{catalog}/{schema}/origem/loja_origem.db"
banco_local = "/tmp/loja_origem.db"  # SQLite escreve melhor em disco local do que direto no Volume

if os.path.exists(banco_volume):
    shutil.copyfile(banco_volume, banco_local)

conn = sqlite3.connect(banco_local)
for l in range(lote + 1):          # aplica do lote 0 até o lote pedido (idempotente)
    aplicar_lote_sqlite(conn, l)

for tabela in ["lojas", "produtos", "clientes", "vendas"]:
    print(tabela, conn.execute(f"SELECT COUNT(*) FROM {tabela}").fetchone()[0], "linhas")
conn.close()

shutil.copyfile(banco_local, banco_volume)
print("Banco salvo em", banco_volume)
