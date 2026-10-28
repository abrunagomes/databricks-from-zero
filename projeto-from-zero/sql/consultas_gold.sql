-- Troque workspace.varejo pelo seu catalog.schema

-- Faturamento por loja
SELECT nome_loja, cidade, ROUND(SUM(faturamento), 2) AS faturamento, SUM(qtd_vendas) AS vendas
FROM workspace.varejo.gold_faturamento_loja_dia
GROUP BY nome_loja, cidade
ORDER BY faturamento DESC;

-- Top 10 produtos
SELECT posicao, nome_produto, categoria, faturamento
FROM workspace.varejo.gold_ranking_produtos
ORDER BY posicao
LIMIT 10;

-- Funil de pedidos
SELECT * FROM workspace.varejo.gold_vendas_por_status;

-- O que foi rejeitado e por quê?
SELECT motivo_rejeicao, COUNT(*) AS qtd
FROM workspace.varejo.silver_vendas_rejeitadas
GROUP BY motivo_rejeicao;

-- Uma venda que mudou de status: todas as versões ficam na Bronze
SELECT id_venda, status, atualizado_em, _ingestao_ts
FROM workspace.varejo.bronze_vendas
WHERE id_venda IN (SELECT id_venda FROM workspace.varejo.bronze_vendas GROUP BY id_venda HAVING COUNT(*) > 1 LIMIT 1)
ORDER BY atualizado_em;

-- Time travel do Delta
DESCRIBE HISTORY workspace.varejo.silver_vendas;
