# Databricks notebook source
# MAGIC %md
# MAGIC # 30 · Treinar o modelo de gols
# MAGIC Ajusta ataque, defesa e mando de campo com os jogos das últimas 5 temporadas + a atual.
# MAGIC Grava os parâmetros com o `run_id` do job: a simulação desta execução lê exatamente estes números.

# COMMAND ----------

import os
import sys

import pandas as pd

sys.path.append(os.path.abspath("../lib"))
from brasileirao import PARAMS, ajustar_forcas, ajustar_perfis, janela_treino, promovidos
from brasileirao.config import Config, parametro, registrar_valor

cfg = Config.from_job(dbutils)
params = {**PARAMS, "meia_vida_dias": float(parametro(dbutils, "meia_vida_dias", str(PARAMS["meia_vida_dias"])))}

partidas = spark.table(cfg.t("silver", "partidas")).select(
    "temporada", "data_hora", "mandante", "visitante", "gols_mandante", "gols_visitante").toPandas()
partidas["data_hora"] = pd.to_datetime(partidas["data_hora"])

# COMMAND ----------

treino = janela_treino(partidas, cfg.temporada_atual, cfg.data_ref, params)
forcas, gerais = ajustar_forcas(treino, cfg.data_ref, promovidos(partidas, cfg.temporada_atual), params)
perfis = ajustar_perfis(treino, cfg.data_ref, params)
print(f"{len(treino)} jogos no treino · mando de campo = +{(2.718281828 ** gerais['mando'] - 1):.0%} de gols")

for df in (forcas, perfis):
    df["run_id"] = cfg.run_id
    df["data_ref"] = cfg.data_ref
forcas["mu"], forcas["mando"] = gerais["mu"], gerais["mando"]

for nome, df in (("modelo_forcas", forcas), ("modelo_perfis", perfis)):
    spark.createDataFrame(df).write.mode("append").option("mergeSchema", "true").saveAsTable(cfg.t("gold", nome))

display(forcas.sort_values("ataque", ascending=False))
registrar_valor(dbutils, "jogos_treino", len(treino))
registrar_valor(dbutils, "mando", round(gerais["mando"], 4))
