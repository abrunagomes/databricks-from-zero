"""Gera os CSVs que simulam a exportação das tabelas do banco (MySQL Workbench etc.).

Uso:  python scripts/gerar_csv.py        ->  pasta dados_csv/
- lojas.csv, produtos.csv, clientes.csv: cadastros
- vendas_lote0.csv: tabela vendas depois da carga inicial (400 linhas)
- vendas_lote1.csv: tabela vendas depois do lote 1 (800 linhas, algumas com status atualizado)
- vendas_lote2.csv: idem, depois do lote 2 (1200 linhas)
Para subir no Volume, envie o arquivo de vendas com o nome vendas.csv (ou informe o nome no widget).
"""
import csv
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import mock_dados as m  # noqa: E402

SAIDA = os.path.join(os.path.dirname(__file__), "..", "dados_csv")


def exportar(conn, tabela, arquivo):
    cursor = conn.execute(f"SELECT {', '.join(m.COLUNAS[tabela])} FROM {tabela} ORDER BY 1")
    with open(os.path.join(SAIDA, arquivo), "w", encoding="utf-8", newline="") as f:
        escritor = csv.writer(f)
        escritor.writerow(m.COLUNAS[tabela])
        escritor.writerows(cursor.fetchall())  # None vira campo vazio
    print("gerado:", arquivo)


if __name__ == "__main__":
    os.makedirs(SAIDA, exist_ok=True)
    conn = sqlite3.connect(":memory:")
    for lote in range(3):
        m.aplicar_lote_sqlite(conn, lote)
        if lote == 0:
            for tabela in ("lojas", "produtos", "clientes"):
                exportar(conn, tabela, f"{tabela}.csv")
        exportar(conn, "vendas", f"vendas_lote{lote}.csv")
