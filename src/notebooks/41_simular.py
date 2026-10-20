# Databricks notebook source
# MAGIC %md
# MAGIC # 41 · Simular o resto do campeonato (Monte Carlo)
# MAGIC Sorteia os jogos restantes N vezes com as chances da tarefa anterior e conta em quantas
# MAGIC tabelas finais cada time foi campeão, ficou no Top 5 ou caiu.

# COMMAND ----------

import json
import os
import sys

sys.path.append(os.path.abspath("../lib"))
from brasileirao import simular
from brasileirao.config import Config, parametro, registrar_valor

cfg = Config.from_job(dbutils)
n = int(parametro(dbutils, "n_simulacoes", "200000"))

classif = spark.table(cfg.t("gold", "classificacao_atual")).toPandas()
probs = spark.table(cfg.t("gold", "prob_jogos")).toPandas()

# COMMAND ----------

proj, resumo = simular(classif, probs, n=n)
proj["run_id"], proj["data_ref"], proj["simulacoes"] = cfg.run_id, cfg.data_ref, n

df = spark.createDataFrame(proj)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.t("gold", "projecao_final"))
df.write.mode("append").option("mergeSchema", "true").saveAsTable(cfg.t("gold", "historico_projecoes"))  # evolução rodada a rodada

display(proj.drop(columns=["dist_posicoes"]))
lider = proj.iloc[0]
registrar_valor(dbutils, "favorito", f"{lider.time} {lider.p_titulo:.1%}")
registrar_valor(dbutils, "resumo", json.dumps({k: round(v, 1) for k, v in resumo.items()}))
