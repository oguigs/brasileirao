# Databricks notebook source
# MAGIC %md
# MAGIC # 31 · Backtest do modelo
# MAGIC Prevê cada semana das temporadas de teste usando só os jogos anteriores a ela e compara com:
# MAGIC a média da liga (sem modelo) e as odds de fechamento das casas de aposta (o mercado).
# MAGIC Métrica: RPS (Ranked Probability Score), quanto menor melhor.
# MAGIC
# MAGIC Rodado num **for each** por temporada ou por `meia_vida_dias`, vira uma busca de parâmetros em paralelo.

# COMMAND ----------

import os
import sys

import numpy as np
import pandas as pd

sys.path.append(os.path.abspath("../lib"))
from brasileirao import PARAMS, backtest, rps
from brasileirao.config import Config, parametro, registrar_valor

cfg = Config.from_job(dbutils)
temporadas = [int(t) for t in parametro(dbutils, "temporadas_teste", "2025").split(",")]
params = {**PARAMS, "meia_vida_dias": float(parametro(dbutils, "meia_vida_dias", str(PARAMS["meia_vida_dias"])))}

p = spark.table(cfg.t("silver", "partidas")).toPandas()
p["data_hora"] = pd.to_datetime(p["data_hora"])
inv = 1 / p[["odd_mandante", "odd_empate", "odd_visitante"]]
p[["mercado_mandante", "mercado_empate", "mercado_visitante"]] = inv.div(inv.sum(axis=1), axis=0).to_numpy()

# COMMAND ----------

prev = backtest(p, temporadas, params)
y = prev["resultado"].to_numpy()
modelo = prev[["p_mandante", "p_empate", "p_visitante"]].to_numpy()
liga = np.tile(np.bincount(y, minlength=3) / len(y), (len(y), 1))  # frequência casa/empate/fora
mercado = prev[["mercado_mandante", "mercado_empate", "mercado_visitante"]].to_numpy()
ok = ~np.isnan(mercado).any(axis=1)

resultado = pd.DataFrame([{
    "run_id": cfg.run_id, "temporadas": ",".join(map(str, temporadas)), "meia_vida_dias": params["meia_vida_dias"],
    "jogos": int(len(y)), "rps_modelo": rps(modelo, y), "rps_media_liga": rps(liga, y),
    "rps_mercado": rps(mercado[ok], y[ok]) if ok.any() else None, "calculado_em": cfg.data_ref}])
spark.createDataFrame(resultado).write.mode("append").option("mergeSchema", "true").saveAsTable(cfg.t("gold", "backtest_metricas"))
display(resultado)
registrar_valor(dbutils, "rps_modelo", round(float(resultado.rps_modelo[0]), 4))
