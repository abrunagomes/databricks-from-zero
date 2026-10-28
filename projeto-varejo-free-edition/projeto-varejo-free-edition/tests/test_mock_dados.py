import sqlite3

import mock_dados as m


def test_aplicar_lote_e_idempotente():
    conn = sqlite3.connect(":memory:")
    m.aplicar_lote_sqlite(conn, 0)
    m.aplicar_lote_sqlite(conn, 0)
    assert conn.execute("SELECT COUNT(*) FROM vendas").fetchone()[0] == 400
    assert conn.execute("SELECT COUNT(*) FROM clientes").fetchone()[0] == 200

    m.aplicar_lote_sqlite(conn, 1)
    assert conn.execute("SELECT COUNT(*) FROM vendas").fetchone()[0] == 800
    atualizadas = conn.execute("SELECT COUNT(*) FROM vendas WHERE id_venda LIKE 'V00%' AND atualizado_em >= '2026-10-02'").fetchone()[0]
    assert atualizadas > 0  # o lote 1 alterou vendas do lote 0


def test_colunas_batem_com_o_banco():
    conn = sqlite3.connect(":memory:")
    m.aplicar_lote_sqlite(conn, 0)
    for tabela, colunas in m.COLUNAS.items():
        reais = [c[1] for c in conn.execute(f"PRAGMA table_info({tabela})")]
        assert reais == colunas
