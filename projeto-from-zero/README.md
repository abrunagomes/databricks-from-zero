# Projeto de engenharia de dados para iniciantes — Databricks Free Edition

Case do Databricks User Group Curitiba: uma rede de lojas quer enxergar vendas, produtos e clientes a partir do banco de dados da operação. Vamos **ingerir um banco de dados**, organizar em camadas Medallion (Bronze, Silver, Gold), versionar no Git e orquestrar em um Job, tudo com computação **serverless** e governança do **Unity Catalog**.

```
Banco de origem ──► Bronze ──────────► Silver ───────────────► Gold
(SQLite ou          cópia fiel,        limpa, padroniza,        faturamento por loja/dia,
 Postgres)          com metadados      versão mais recente,     ranking de produtos,
                    e incremental      rejeitados à parte       funil de pedidos
```

## Sobre a origem dos dados (leia primeiro)

A Free Edition roda só em compute serverless e restringe a saída de internet a uma lista de domínios confiáveis. Por isso, conectar em um banco externo pode falhar, e um banco na sua máquina (MySQL Workbench, pgAdmin) não é visível para o workspace. O projeto tem quatro origens, escolhidas no widget `origem`:

| Origem | Como funciona | Funciona na Free Edition? |
|---|---|---|
| `csv` (padrão) | Você cria as tabelas no seu banco (MySQL Workbench, pgAdmin...), exporta cada tabela para CSV e envia ao Volume `origem_csv`. A Bronze lê os arquivos com a mesma lógica incremental do banco. | Sim, não depende de internet |
| `sqlite` | O notebook 00 cria um banco SQLite real (tabelas, chaves, UPDATEs) em um Volume do Unity Catalog. A ingestão lê dele com SQL, como leria de qualquer banco. | Sim, não depende de internet |
| `mysql` (opcional) | Ingestão via JDBC em um MySQL na nuvem, populado com `sql/mysql_*.sql`. | Depende de o workspace conseguir sair para o host do banco. Teste antes da apresentação |
| `postgres` (opcional) | Igual ao MySQL, com `sql/postgres_*.sql`. | Mesma condição |

## Estrutura

| Caminho | Função |
|---|---|
| `src/mock_dados.py` | Gera o banco mock (lojas, produtos, clientes, vendas) com sujeira proposital |
| `src/00_criar_banco_origem.py` | Cria o banco SQLite de origem no Volume. Parâmetro `lote` simula novos dias |
| `src/01_bronze.py` | Ingestão: cadastros em carga completa, vendas incremental por `atualizado_em` |
| `src/02_silver.py` | Limpeza, última versão de cada venda, rejeitados, regra de qualidade |
| `src/03_gold.py` | 3 tabelas de consumo + checagens |
| `src/transformacoes.py` | Regras de negócio em funções puras, reaproveitadas pelos testes |
| `dados_csv/` | CSVs prontos (lojas, produtos, clientes e 3 snapshots de vendas) |
| `scripts/gerar_csv.py` | Regenera os CSVs |
| `sql/mysql_*.sql`, `sql/postgres_*.sql` | Schema e dados para popular um MySQL ou Postgres (origens `mysql` e `postgres`) |
| `sql/consultas_gold.sql` | Consultas para a demo |
| `scripts/gerar_sql.py` | Regenera os SQLs do MySQL e do Postgres |
| `tests/` | Testes com pytest (rodam fora do Databricks) |

O banco mock tem: 10 lojas, 20 produtos, 200 clientes e 400 vendas por lote. Os problemas propositais são e-mails em branco ou em caixa alta, cidades em maiúsculas, status em minúsculas, desconto nulo, quantidade zero ou negativa e vendas que mudam de status depois.

## Passo a passo

### 1. Git
1. Crie um repositório no GitHub e envie esta pasta (`git init`, `git add .`, `git commit`, `git push`).
2. No Databricks: **Workspace → Create → Git folder**, cole a URL do repositório. Repositório privado exige vincular o GitHub (token) em *Settings → Linked accounts*.
3. A partir daqui, edite nos notebooks, faça commit e push pela própria interface do Git folder.

### 2. Rodar na mão (para explicar cada etapa)
1. Abra `src/00_criar_banco_origem.py`, confira `catalog` e `schema` nos widgets, e execute com `lote = 0`. Em *Catalog* confira o nome do seu catálogo (na Free Edition costuma ser `workspace`).
2. Execute `01_bronze`, depois `02_silver` e `03_gold`. Em *Catalog* explore as tabelas `bronze_*`, `silver_*`, `gold_*`.
3. Rode as consultas de `sql/consultas_gold.sql` no SQL Editor.

### 3. Mostrar a ingestão incremental
1. Rode `00_criar_banco_origem` com `lote = 1`: entram 400 vendas novas e várias vendas do dia anterior mudam de status.
2. Rode `01_bronze` de novo. O log mostra a marca d'água e só as linhas novas ou alteradas entram.
3. Rode a consulta "uma venda que mudou de status" do SQL: a Bronze guarda todas as versões. Depois rode `02_silver` e mostre que a Silver fica só com a mais recente.

### 4. Orquestrar no Job (Jobs & Pipelines)
1. **Jobs & Pipelines → Create → Job**, com 4 tarefas do tipo *Notebook* apontando para os notebooks do Git folder, encadeadas: `criar_banco` → `bronze` → `silver` → `gold`.
2. Em *Job parameters*, crie `catalog`, `schema`, `lote` e `origem`. Parâmetros de job com o mesmo nome dos widgets chegam aos notebooks.
3. O compute é serverless, sem cluster para configurar. Rode com `lote = 0`, depois use *Run now with different parameters* com `lote = 1`.
4. Opcional: agendamento (*Add trigger*) e notificação de falha por e-mail.

### 5. Origem CSV (banco local exportado)
1. Crie as tabelas no seu banco com `sql/mysql_00_schema_e_lote0.sql` (ou o `postgres_...`).
2. Exporte cada tabela para CSV. No MySQL Workbench: botão direito na tabela → *Table Data Export Wizard* → CSV, com cabeçalho. Ou use direto os arquivos prontos de `dados_csv/`.
3. No Databricks, em *Catalog* → seu schema, crie o Volume `origem_csv` (rodar o `01_bronze` com `origem = csv` também o cria) e envie `lojas.csv`, `produtos.csv`, `clientes.csv` e o arquivo de vendas com o nome `vendas.csv` (ou informe outro nome no widget `arquivo_vendas`).
4. Rode `01_bronze` com `origem = csv`: 400 vendas entram.
5. Para a demo do incremental: rode `sql/mysql_lote1.sql` no banco, exporte `vendas` de novo (ou use `vendas_lote1.csv`), envie sobrescrevendo `vendas.csv` e rode o `01_bronze`. Só as linhas novas ou alteradas entram. Cada exportação é um snapshot da tabela inteira.

### 6. Opcional: origem MySQL ou Postgres
1. Crie o banco, rode `sql/mysql_00_schema_e_lote0.sql` (ou o `postgres_...`) e, depois, `mysql_lote1.sql` para simular novos dias. No MySQL Workbench: *File → Open SQL Script*, depois o raio para executar tudo.
2. Rode `01_bronze` com `origem = mysql` (ou `postgres`) e os widgets `db_host`, `db_porta`, `db_banco`, `db_usuario` e `db_senha`. Use um banco descartável: senha em widget aparece em texto puro. O código tenta antes `dbutils.secrets.get("varejo", "db_senha")`.
3. Ao trocar a origem, use outro `schema` (ou apague as tabelas `bronze_*`) para não misturar versões.

## Pontos para explicar: serverless e Unity Catalog

**Serverless**
- Não existe cluster para criar, dimensionar ou desligar: o Databricks entrega o compute sob demanda, em segundos.
- O código é o mesmo; o que muda é quem gerencia a infraestrutura.
- Na Free Edition é a única opção, com cotas de uso e no máximo 5 tarefas de job simultâneas.

**Unity Catalog**
- Hierarquia `catalog.schema.tabela`. Neste projeto: `workspace.varejo.bronze_vendas`.
- Volumes guardam arquivos (aqui, o banco SQLite) sob a mesma governança das tabelas.
- Em *Catalog Explorer → Lineage* dá para ver de qual tabela cada Gold veio.
- Permissões por catálogo, schema e tabela, em um lugar só.

## Roteiro de 60 minutos

| Min | Bloco |
|---|---|
| 0–5 | Abertura e o case |
| 5–12 | Conceitos: Lakehouse, Delta, Unity Catalog e serverless |
| 12–17 | Arquitetura Medallion e o banco de origem |
| 17–27 | Bronze: ingestão do banco, carga completa e incremental |
| 27–37 | Silver: qualidade e rejeitados |
| 37–42 | Gold: tabelas de consumo e consultas |
| 42–50 | Git folder e Job (demo do lote 1) |
| 50–60 | DataOps (testes), próximos passos e perguntas |

## Testes locais (opcional)

```
pip install -r requirements-dev.txt   # requer Java
pytest -q
```

## Próximos passos

- Trocar `overwrite` por `MERGE` na Silver
- Migrar para Lakeflow Spark Declarative Pipelines com expectations
- Dashboard AI/BI sobre as tabelas Gold
- Databricks Asset Bundles e CI/CD no GitHub
