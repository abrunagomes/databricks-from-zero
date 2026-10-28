import csv
import sqlite3

import mock_dados as m
from transformacoes import tipar_csv


def test_tipar_csv_trata_nulos_do_workbench(spark, tmp_path):
    arquivo = tmp_path / "vendas.csv"
    linhas = [
        m.COLUNAS["vendas"],
        ["V1", "L01", "C1", "P01", "2026-10-01", "2", "10.5", "NULL", "pago", "2026-10-01 09:00:00"],
        ["V2", "L01", "C1", "P01", "2026-10-01", "1", "10.5", "", "PAGO", "2026-10-01 09:00:00"],
        ["V3", "L01", "C1", "P01", "2026-10-01", "x", "10.5", "0.1", "PAGO", "2026-10-01 09:00:00"],
    ]
    with open(arquivo, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(linhas)

    bruto = spark.read.option("header", "true").csv(str(arquivo))
    df = tipar_csv(bruto, m.SCHEMAS_SPARK["vendas"])
    r = {x.id_venda: x for x in df.collect()}

    assert r["V1"].desconto is None and r["V1"].quantidade == 2 and r["V1"].preco_unitario == 10.5
    assert r["V2"].desconto is None
    assert r["V3"].quantidade is None          # valor inválido vira null, não derruba o job
    assert dict(df.dtypes)["quantidade"] == "int" and dict(df.dtypes)["desconto"] == "double"
