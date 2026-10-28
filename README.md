# Projeto de engenharia de dados para iniciantes — Databricks Free Edition

Case do Databricks User Group Curitiba: uma rede de lojas quer enxergar vendas, produtos e clientes a partir do banco de dados da operação. Vamos **ingerir um banco de dados**, organizar em camadas Medallion (Bronze, Silver, Gold), versionar no Git e orquestrar em um Job, tudo com computação **serverless** e governança do **Unity Catalog**.

## Sobre a origem dos dados

A Free Edition roda só em compute serverless e restringe a saída de internet a uma lista de domínios confiáveis. Por isso, conectar em um banco externo pode falhar, e um banco local não é visível para o workspace. O projeto tem origem de arquivos CSV com dados fictícios:

## Estrutura

| Caminho | Função |
|---|---|
| `src/mock_dados.py` | Gera tabela mock (lojas, produtos, clientes, vendas) com dados sujos proposital |
| `src/_bronze.py` | Ingestão: cadastros em carga completa, vendas incremental por `atualizado_em` |
| `src/_silver.py` | Limpeza, última versão de cada venda, rejeitados, regra de qualidade |
| `src/_gold.py` | 3 tabelas de consumo + checagens |
| `src/transformacoes.py` | Regras de negócio em funções puras, reaproveitadas pelos testes |
| `dados_csv/` | CSVs prontos (lojas, produtos, clientes e 3 snapshots de vendas) |
| `scripts/gerar_csv.py` | Regenera os CSVs |
| `sql/consultas_gold.sql` | Consultas para a demo |
| `tests/` | Testes com pytest (rodam fora do Databricks) |

O banco mock tem: 10 lojas, 20 produtos, 200 clientes e 400 vendas por lote. Os problemas propositais são e-mails em branco ou em caixa alta, cidades em maiúsculas, status em minúsculas, desconto nulo, quantidade zero ou negativa e vendas que mudam de status depois.

## Passo a passo

### 1. Git
1. Crie um repositório no GitHub e envie esta pasta (`git init`, `git add .`, `git commit`, `git push`).
2. No Databricks: **Workspace → Create → Git folder**, cole a URL do repositório. Repositório privado exige vincular o GitHub (token) em *Settings → Linked accounts*.
3. A partir daqui, edite nos notebooks, faça commit e push pela própria interface do Git folder.

### 2. Rodar na mão (para explicar cada etapa)
1. Em *Catalog* confira o nome do seu catálogo, crie o schema e o volume.
2. Execute `bronze`, depois `silver` e `gold` no workspace.
3. Rode as consultas de `sql/consultas_gold.sql` no SQL Editor para validação de dados.

### 3. Orquestrar no Job (Jobs & Pipelines)
1. **Jobs & Pipelines → Create → Job**, com 4 tarefas do tipo *Notebook* apontando para os notebooks do Git folder: `bronze` → `silver` → `gold`.
2. Em *Job parameters*, crie `catalog`, `schema` e `volume`. Parâmetros de job com o mesmo nome dos widgets chegam aos notebooks.
3. O compute é serverless, sem cluster para configurar. Rode com `lote = 0`, depois use *Run now with different parameters* com `lote = 1`.
4. Opcional: agendamento (*Add trigger*) e notificação de falha por e-mail.


## Serverless e Unity Catalog

**Serverless**
- Não existe cluster para criar, dimensionar ou desligar: o Databricks entrega o compute sob demanda, em segundos.
- O código é o mesmo; o que muda é quem gerencia a infraestrutura.
- Na Free Edition é a única opção, com cotas de uso e no máximo 5 tarefas de job simultâneas.

**Unity Catalog**
- Hierarquia `catalog.schema.tabela`. Neste projeto: `user_group.project_from_zero.bronze_vendas`.
- Volumes guardam arquivos sob a mesma governança das tabelas.
- Em *Catalog Explorer → Lineage* dá para ver de qual tabela cada Gold veio.
- Permissões por catálogo, schema e tabela, em um lugar só.

## Testes locais (opcional)

```
pip install -r requirements-dev.txt   # requer Java
pytest -q
```
