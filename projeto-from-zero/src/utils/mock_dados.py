"""Gera o banco de dados MOCK da rede de lojas (sem Spark, só Python puro).

Usado por:
  - src/00_criar_banco_origem.py  -> cria um banco SQLite dentro do Databricks
  - scripts/gerar_sql_postgres.py -> gera SQL para um Postgres gratuito (Neon, Supabase...)

Tudo é determinístico: rodar duas vezes gera exatamente os mesmos dados.
"""
import random
from datetime import datetime, timedelta

BASE = datetime(2026, 10, 1)

LOJAS = [
    ("L01", "Curitiba Centro", "Curitiba", "PR"), ("L02", "Curitiba Batel", "Curitiba", "PR"),
    ("L03", "Londrina Gleba", "Londrina", "PR"), ("L04", "Maringá Zona 7", "Maringá", "PR"),
    ("L05", "Cascavel Centro", "Cascavel", "PR"), ("L06", "Ponta Grossa Oficinas", "Ponta Grossa", "PR"),
    ("L07", "Joinville Centro", "Joinville", "SC"), ("L08", "Florianópolis Trindade", "Florianópolis", "SC"),
    ("L09", "Porto Alegre Moinhos", "Porto Alegre", "RS"), ("L10", "São Paulo Pinheiros", "São Paulo", "SP"),
]

PRODUTOS = [
    ("P01", "Café torrado 500g", "Mercearia", 18.9), ("P02", "Arroz 5kg", "Mercearia", 29.9),
    ("P03", "Feijão 1kg", "Mercearia", 8.5), ("P04", "Leite integral 1L", "Laticínios", 5.2),
    ("P05", "Queijo mussarela 500g", "Laticínios", 24.0), ("P06", "Iogurte natural", "Laticínios", 3.8),
    ("P07", "Detergente", "Limpeza", 2.9), ("P08", "Sabão em pó 1kg", "Limpeza", 14.5),
    ("P09", "Desinfetante 2L", "Limpeza", 9.9), ("P10", "Shampoo 350ml", "Higiene", 16.9),
    ("P11", "Sabonete", "Higiene", 2.5), ("P12", "Creme dental", "Higiene", 5.9),
    ("P13", "Refrigerante 2L", "Bebidas", 9.5), ("P14", "Suco de uva 1L", "Bebidas", 11.9),
    ("P15", "Água mineral 1,5L", "Bebidas", 3.2), ("P16", "Pão de forma", "Padaria", 8.9),
    ("P17", "Biscoito recheado", "Padaria", 4.2), ("P18", "Macarrão 500g", "Mercearia", 5.5),
    ("P19", "Óleo de soja 900ml", "Mercearia", 7.9), ("P20", "Açúcar 1kg", "Mercearia", 5.1),
]

NOMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Fábio", "Gabriela", "Henrique", "Isabela", "João",
         "Karina", "Lucas", "Mariana", "Nicolas", "Olívia", "Paulo", "Renata", "Samuel", "Tatiana", "Vitor"]
SOBRENOMES = ["Silva", "Souza", "Oliveira", "Santos", "Pereira", "Costa", "Rodrigues", "Almeida", "Nunes", "Lima"]
CIDADES = ["Curitiba", "Londrina", "Maringá", "Cascavel", "Ponta Grossa", "Joinville", "Florianópolis"]

# Colunas lidas de cada tabela e schema do Spark (usados na ingestão)
COLUNAS = {
    "lojas": ["id_loja", "nome_loja", "cidade", "uf"],
    "produtos": ["id_produto", "nome_produto", "categoria", "preco_lista"],
    "clientes": ["id_cliente", "nome", "email", "cidade", "criado_em"],
    "vendas": ["id_venda", "id_loja", "id_cliente", "id_produto", "data_venda",
               "quantidade", "preco_unitario", "desconto", "status", "atualizado_em"],
}
SCHEMAS_SPARK = {
    "lojas": "id_loja string, nome_loja string, cidade string, uf string",
    "produtos": "id_produto string, nome_produto string, categoria string, preco_lista double",
    "clientes": "id_cliente string, nome string, email string, cidade string, criado_em string",
    "vendas": ("id_venda string, id_loja string, id_cliente string, id_produto string, data_venda string, "
               "quantidade int, preco_unitario double, desconto double, status string, atualizado_em string"),
}

DDL_SQLITE = """
CREATE TABLE IF NOT EXISTS lojas (id_loja TEXT PRIMARY KEY, nome_loja TEXT, cidade TEXT, uf TEXT);
CREATE TABLE IF NOT EXISTS produtos (id_produto TEXT PRIMARY KEY, nome_produto TEXT, categoria TEXT, preco_lista REAL);
CREATE TABLE IF NOT EXISTS clientes (id_cliente TEXT PRIMARY KEY, nome TEXT, email TEXT, cidade TEXT, criado_em TEXT);
CREATE TABLE IF NOT EXISTS vendas (
    id_venda TEXT PRIMARY KEY, id_loja TEXT, id_cliente TEXT, id_produto TEXT, data_venda TEXT,
    quantidade INTEGER, preco_unitario REAL, desconto REAL, status TEXT, atualizado_em TEXT
);
"""

DDL_POSTGRES = """
CREATE TABLE IF NOT EXISTS lojas (id_loja VARCHAR(10) PRIMARY KEY, nome_loja VARCHAR(80), cidade VARCHAR(60), uf CHAR(2));
CREATE TABLE IF NOT EXISTS produtos (id_produto VARCHAR(10) PRIMARY KEY, nome_produto VARCHAR(80), categoria VARCHAR(40), preco_lista NUMERIC(10,2));
CREATE TABLE IF NOT EXISTS clientes (id_cliente VARCHAR(10) PRIMARY KEY, nome VARCHAR(80), email VARCHAR(120), cidade VARCHAR(60), criado_em DATE);
CREATE TABLE IF NOT EXISTS vendas (
    id_venda VARCHAR(12) PRIMARY KEY, id_loja VARCHAR(10), id_cliente VARCHAR(10), id_produto VARCHAR(10),
    data_venda DATE, quantidade INTEGER, preco_unitario NUMERIC(10,2), desconto NUMERIC(4,2),
    status VARCHAR(20), atualizado_em TIMESTAMP
);
"""

DDL_MYSQL = """
CREATE DATABASE IF NOT EXISTS varejo CHARACTER SET utf8mb4;
USE varejo;
CREATE TABLE IF NOT EXISTS lojas (id_loja VARCHAR(10) PRIMARY KEY, nome_loja VARCHAR(80), cidade VARCHAR(60), uf CHAR(2));
CREATE TABLE IF NOT EXISTS produtos (id_produto VARCHAR(10) PRIMARY KEY, nome_produto VARCHAR(80), categoria VARCHAR(40), preco_lista DECIMAL(10,2));
CREATE TABLE IF NOT EXISTS clientes (id_cliente VARCHAR(10) PRIMARY KEY, nome VARCHAR(80), email VARCHAR(120), cidade VARCHAR(60), criado_em DATE);
CREATE TABLE IF NOT EXISTS vendas (
    id_venda VARCHAR(12) PRIMARY KEY, id_loja VARCHAR(10), id_cliente VARCHAR(10), id_produto VARCHAR(10),
    data_venda DATE, quantidade INT, preco_unitario DECIMAL(10,2), desconto DECIMAL(4,2),
    status VARCHAR(20), atualizado_em DATETIME
);
"""


def gerar_clientes(n: int = 200):
    rng = random.Random(42)
    linhas = []
    for i in range(1, n + 1):
        nome, sobrenome = rng.choice(NOMES), rng.choice(SOBRENOMES)
        email = f"{nome}.{sobrenome}{i}@exemplo.com".lower()
        sorteio = rng.random()
        if sorteio < 0.05:
            email = None                    # sujeira: e-mail ausente
        elif sorteio < 0.12:
            email = f"  {email.upper()} "   # sujeira: espaços e maiúsculas
        cidade = rng.choice(CIDADES)
        if rng.random() < 0.08:
            cidade = cidade.upper()         # sujeira: cidade em caixa alta
        criado_em = (BASE - timedelta(days=rng.randint(30, 700))).strftime("%Y-%m-%d")
        linhas.append((f"C{i:04d}", f"{nome} {sobrenome}", email, cidade, criado_em))
    return linhas


def gerar_vendas_lote(lote: int, n: int = 400):
    """Vendas do 'dia' `lote` (lote 0 = 01/10/2026, lote 1 = 02/10/2026...)."""
    rng = random.Random(1000 + lote)
    inicio = BASE + timedelta(days=lote)
    linhas = []
    for i in range(n):
        momento = inicio + timedelta(seconds=rng.randint(0, 86399))
        produto = rng.choice(PRODUTOS)
        status = rng.choice(["CRIADO", "PAGO", "PAGO", "ENVIADO", "CANCELADO"])
        if rng.random() < 0.03:
            status = status.lower()         # sujeira: status em minúsculas
        quantidade = rng.randint(1, 5)
        if rng.random() < 0.01:
            quantidade = rng.choice([0, -1])  # sujeira: quantidade inválida
        linhas.append((
            f"V{lote:02d}{i:04d}", rng.choice(LOJAS)[0], f"C{rng.randint(1, 200):04d}", produto[0],
            momento.strftime("%Y-%m-%d"), quantidade,
            round(produto[3] * rng.uniform(0.95, 1.05), 2),
            rng.choice([0.0, 0.0, 0.0, 0.05, 0.1, None]),  # None = desconto não informado
            status, momento.strftime("%Y-%m-%d %H:%M:%S"),
        ))
    return linhas


def gerar_atualizacoes(lote: int):
    """Vendas do lote anterior mudam de status (CRIADO->PAGO, PAGO->ENVIADO): simula UPDATEs no banco."""
    if lote == 0:
        return []
    rng = random.Random(5000 + lote)
    proximo = {"CRIADO": "PAGO", "PAGO": "ENVIADO"}
    inicio = BASE + timedelta(days=lote)
    atualizacoes = []
    for venda in gerar_vendas_lote(lote - 1):
        status = venda[8].upper()
        if status in proximo and rng.random() < 0.5:
            momento = inicio + timedelta(seconds=rng.randint(0, 86399))
            atualizacoes.append((venda[0], proximo[status], momento.strftime("%Y-%m-%d %H:%M:%S")))
    return atualizacoes


def aplicar_lote_sqlite(conn, lote: int):
    """Aplica o lote no SQLite. Idempotente: pode rodar várias vezes sem duplicar."""
    conn.executescript(DDL_SQLITE)
    if lote == 0:
        conn.executemany("INSERT OR REPLACE INTO lojas VALUES (?,?,?,?)", LOJAS)
        conn.executemany("INSERT OR REPLACE INTO produtos VALUES (?,?,?,?)", PRODUTOS)
        conn.executemany("INSERT OR REPLACE INTO clientes VALUES (?,?,?,?,?)", gerar_clientes())
    conn.executemany("INSERT OR REPLACE INTO vendas VALUES (?,?,?,?,?,?,?,?,?,?)", gerar_vendas_lote(lote))
    conn.executemany(
        "UPDATE vendas SET status = ?, atualizado_em = ? WHERE id_venda = ?",
        [(status, momento, id_venda) for id_venda, status, momento in gerar_atualizacoes(lote)],
    )
    conn.commit()
