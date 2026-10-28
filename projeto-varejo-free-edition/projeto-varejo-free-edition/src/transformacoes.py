"""Regras de negócio das camadas Silver e Gold (funções puras, testáveis com pytest)."""
from pyspark.sql import DataFrame, Window, functions as F


# ---------- BRONZE (origem CSV) ----------
def tipar_csv(df: DataFrame, schema_ddl: str) -> DataFrame:
    """CSV chega tudo como texto. Troca '', 'NULL' e '\\N' (como o MySQL Workbench exporta) por null
    e converte para os tipos da Bronze; valores inválidos viram null em vez de quebrar o job."""
    pares = [c.strip().split(" ") for c in schema_ddl.split(",")]
    limpo = df.select([
        F.when(F.trim(F.col(c)).isin("", "NULL", "\\N"), F.lit(None)).otherwise(F.col(c)).alias(c)
        for c, _ in pares
    ])
    return limpo.select([F.expr(f"try_cast(`{c}` AS {t})").alias(c) for c, t in pares])


# ---------- SILVER ----------
def limpar_clientes(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("email", F.lower(F.trim("email")))
        .withColumn("cidade", F.initcap(F.trim("cidade")))
        .dropDuplicates(["id_cliente"])
        .select("id_cliente", "nome", "email", "cidade", F.to_date("criado_em").alias("criado_em"))
    )


def limpar_vendas(bronze: DataFrame):
    """Retorna (vendas_validas, vendas_rejeitadas)."""
    tipada = (
        bronze.withColumn("status", F.upper(F.trim("status")))
        .withColumn("desconto", F.coalesce("desconto", F.lit(0.0)))
        .withColumn("data_venda", F.to_date("data_venda"))
        .withColumn("atualizado_em", F.to_timestamp("atualizado_em"))
    )
    # A Bronze guarda cada versão do pedido; aqui ficamos só com a mais recente.
    janela = Window.partitionBy("id_venda").orderBy(F.col("atualizado_em").desc(), F.col("_ingestao_ts").desc())
    ultima_versao = tipada.withColumn("_rn", F.row_number().over(janela)).filter("_rn = 1").drop("_rn")

    motivo = (
        F.when(F.col("quantidade") <= 0, "quantidade_invalida")
        .when(F.col("preco_unitario") <= 0, "preco_invalido")
    )
    classificada = ultima_versao.withColumn("motivo_rejeicao", motivo)
    rejeitadas = classificada.filter("motivo_rejeicao IS NOT NULL")
    validas = (
        classificada.filter("motivo_rejeicao IS NULL").drop("motivo_rejeicao")
        .withColumn(
            "valor_total",
            F.round(F.col("quantidade") * F.col("preco_unitario") * (1 - F.col("desconto")), 2),
        )
    )
    return validas, rejeitadas


def verificar_taxa_rejeicao(total: int, rejeitadas: int, limite: float = 0.10) -> float:
    taxa = rejeitadas / total if total else 0.0
    if taxa > limite:
        raise ValueError(f"Taxa de rejeição {taxa:.1%} acima do limite de {limite:.0%}")
    return taxa


# ---------- GOLD ----------
def faturamento_loja_dia(vendas: DataFrame, lojas: DataFrame) -> DataFrame:
    return (
        vendas.filter("status != 'CANCELADO'")
        .join(lojas, "id_loja", "left")
        .groupBy("data_venda", "id_loja", "nome_loja", "cidade", "uf")
        .agg(
            F.round(F.sum("valor_total"), 2).alias("faturamento"),
            F.countDistinct("id_venda").alias("qtd_vendas"),
            F.sum("quantidade").alias("itens_vendidos"),
        )
        .withColumn("ticket_medio", F.round(F.col("faturamento") / F.col("qtd_vendas"), 2))
    )


def ranking_produtos(vendas: DataFrame, produtos: DataFrame) -> DataFrame:
    agregado = (
        vendas.filter("status != 'CANCELADO'")
        .join(produtos, "id_produto", "left")
        .groupBy("id_produto", "nome_produto", "categoria")
        .agg(
            F.round(F.sum("valor_total"), 2).alias("faturamento"),
            F.sum("quantidade").alias("itens_vendidos"),
        )
    )
    return agregado.withColumn("posicao", F.row_number().over(Window.orderBy(F.col("faturamento").desc())))


def vendas_por_status(vendas: DataFrame) -> DataFrame:
    return (
        vendas.groupBy("status")
        .agg(F.count("*").alias("qtd_vendas"), F.round(F.sum("valor_total"), 2).alias("valor_total"))
        .orderBy(F.desc("qtd_vendas"))
    )
