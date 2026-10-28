from datetime import datetime

import pytest

from transformacoes import (faturamento_loja_dia, limpar_clientes, limpar_vendas,
                            ranking_produtos, verificar_taxa_rejeicao)

COLS_VENDAS = "id_venda string, id_loja string, id_cliente string, id_produto string, data_venda string, " \
              "quantidade int, preco_unitario double, desconto double, status string, atualizado_em string, _ingestao_ts timestamp"
T0, T1 = datetime(2026, 10, 1, 8), datetime(2026, 10, 2, 8)


def test_limpar_vendas(spark):
    bronze = spark.createDataFrame([
        ("V1", "L01", "C1", "P01", "2026-10-01", 2, 10.0, 0.1, "criado", "2026-10-01 09:00:00", T0),  # versão antiga
        ("V1", "L01", "C1", "P01", "2026-10-01", 2, 10.0, 0.1, "PAGO", "2026-10-02 09:00:00", T1),    # versão nova
        ("V2", "L01", "C1", "P02", "2026-10-01", 1, 20.0, None, "pago", "2026-10-01 10:00:00", T0),   # desconto nulo
        ("V3", "L02", "C2", "P03", "2026-10-01", 0, 5.0, 0.0, "PAGO", "2026-10-01 11:00:00", T0),     # inválida
    ], COLS_VENDAS)

    validas, rejeitadas = limpar_vendas(bronze)
    v = {r.id_venda: r for r in validas.collect()}

    assert sorted(v) == ["V1", "V2"]
    assert v["V1"].status == "PAGO" and v["V1"].valor_total == 18.0   # ficou a versão mais recente
    assert v["V2"].desconto == 0.0 and v["V2"].status == "PAGO"
    assert [r.motivo_rejeicao for r in rejeitadas.collect()] == ["quantidade_invalida"]


def test_limpar_clientes(spark):
    df = spark.createDataFrame(
        [("C1", "Ana Silva", "  ANA@EXEMPLO.COM ", "CURITIBA", "2026-01-01")],
        "id_cliente string, nome string, email string, cidade string, criado_em string",
    )
    r = limpar_clientes(df).first()
    assert r.email == "ana@exemplo.com" and r.cidade == "Curitiba"


def test_gold_ignora_cancelados(spark):
    vendas = spark.createDataFrame([
        ("V1", "L01", "P01", "2026-10-01", 2, "PAGO", 20.0),
        ("V2", "L01", "P01", "2026-10-01", 1, "CANCELADO", 99.0),
    ], "id_venda string, id_loja string, id_produto string, data_venda string, quantidade int, status string, valor_total double")
    lojas = spark.createDataFrame([("L01", "Centro", "Curitiba", "PR")], "id_loja string, nome_loja string, cidade string, uf string")
    produtos = spark.createDataFrame([("P01", "Café", "Mercearia")], "id_produto string, nome_produto string, categoria string")

    assert faturamento_loja_dia(vendas, lojas).first().faturamento == 20.0
    assert ranking_produtos(vendas, produtos).first().posicao == 1


def test_taxa_rejeicao():
    assert verificar_taxa_rejeicao(100, 5) == 0.05
    with pytest.raises(ValueError):
        verificar_taxa_rejeicao(100, 30)
