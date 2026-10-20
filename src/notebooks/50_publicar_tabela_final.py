# Databricks notebook source
# MAGIC %md
# MAGIC # 50 · Publicar a tabela final
# MAGIC Junta classificação atual + projeção (probabilidades) + limites matemáticos numa só tabela,
# MAGIC a que o painel e os analistas consomem: `gold.tabela_final`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../lib"))
from brasileirao.config import Config

cfg = Config.from_job(dbutils)

spark.sql(f"""
CREATE OR REPLACE TABLE {cfg.t('gold', 'tabela_final')}
COMMENT 'Brasileirão: classificação atual, chances (Monte Carlo) e limites matemáticos por time'
AS
SELECT c.posicao, c.time, c.jogos, c.pontos, c.vitorias, c.empates, c.derrotas, c.saldo,
       l.jogos_restantes,
       round(p.pontos_esperados, 1)       AS pontos_esperados,
       p.posicao_mais_provavel,
       round(100 * p.p_titulo, 1)         AS pct_titulo,
       round(100 * p.p_top5, 1)           AS pct_top5,
       round(100 * p.p_z4, 1)             AS pct_rebaixamento,
       l.melhor_posicao, l.pior_posicao, l.pontos_maximos,
       p.simulacoes, p.run_id, p.data_ref AS calculado_em
FROM {cfg.t('gold', 'classificacao_atual')} c
JOIN {cfg.t('gold', 'projecao_final')} p USING (time)
JOIN {cfg.t('gold', 'limites_matematicos')} l USING (time)
ORDER BY c.posicao
""")
display(spark.table(cfg.t("gold", "tabela_final")))
