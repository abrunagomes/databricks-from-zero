"""Gera os arquivos SQL para popular um banco de origem (MySQL ou Postgres).

Uso:  python scripts/gerar_sql.py
Saída (pasta sql/): mysql_00_schema_e_lote0.sql, mysql_lote1.sql, mysql_lote2.sql
                    postgres_00_schema_e_lote0.sql, postgres_lote1.sql, postgres_lote2.sql
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import mock_dados as m  # noqa: E402

SAIDA = os.path.join(os.path.dirname(__file__), "..", "sql")


def lit(valor):
    if valor is None:
        return "NULL"
    if isinstance(valor, (int, float)):
        return str(valor)
    return "'" + str(valor).replace("'", "''") + "'"


def insert(dialeto, tabela, colunas, linhas, chave):
    valores = ",\n  ".join("(" + ", ".join(lit(v) for v in l) + ")" for l in linhas)
    if dialeto == "mysql":
        sets = ", ".join(f"{c} = VALUES({c})" for c in colunas if c != chave)
        final = f"ON DUPLICATE KEY UPDATE {sets};"
    else:
        sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in colunas if c != chave)
        final = f"ON CONFLICT ({chave}) DO UPDATE SET {sets};"
    return f"INSERT INTO {tabela} ({', '.join(colunas)}) VALUES\n  {valores}\n{final}\n\n"


def lote_sql(dialeto, lote):
    sql = insert(dialeto, "vendas", m.COLUNAS["vendas"], m.gerar_vendas_lote(lote), "id_venda")
    for id_venda, status, momento in m.gerar_atualizacoes(lote):
        sql += (f"UPDATE vendas SET status = {lit(status)}, atualizado_em = {lit(momento)} "
                f"WHERE id_venda = {lit(id_venda)};\n")
    return sql


def escrever(nome, conteudo):
    with open(os.path.join(SAIDA, nome), "w", encoding="utf-8") as f:
        f.write(conteudo)
    print("gerado:", nome)


if __name__ == "__main__":
    for dialeto, ddl, uso in [("mysql", m.DDL_MYSQL, "USE varejo;\n"), ("postgres", m.DDL_POSTGRES, "")]:
        base = (f"-- Banco de origem MOCK da rede de lojas ({dialeto})\n" + ddl + "\n"
                + insert(dialeto, "lojas", m.COLUNAS["lojas"], m.LOJAS, "id_loja")
                + insert(dialeto, "produtos", m.COLUNAS["produtos"], m.PRODUTOS, "id_produto")
                + insert(dialeto, "clientes", m.COLUNAS["clientes"], m.gerar_clientes(), "id_cliente")
                + lote_sql(dialeto, 0))
        escrever(f"{dialeto}_00_schema_e_lote0.sql", base)
        for lote in (1, 2):
            escrever(f"{dialeto}_lote{lote}.sql",
                     f"-- Novas vendas e atualizações do dia {lote}\n{uso}" + lote_sql(dialeto, lote))
