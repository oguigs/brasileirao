# Databricks notebook source
# MAGIC %md
# MAGIC # 40 · Probabilidade de cada jogo restante
# MAGIC Usa os parâmetros gravados pela tarefa `treinar_modelo` **nesta mesma execução** (filtro por `run_id`).

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../lib"))
from brasileirao import prob_jogos
from brasileirao.config import Config, registrar_valor

cfg = Config.from_job(dbutils)


def ultimo_modelo(tabela):
    df = spark.table(cfg.t("gold", tabela))
    run = cfg.run_id if df.filter(f"run_id = '{cfg.run_id}'").limit(1).count() else \
        df.orderBy("data_ref", ascending=False).first()["run_id"]  # rodando à mão: pega o último treino
    return df.filter(f"run_id = '{run}'").toPandas(), run


forcas, run_modelo = ultimo_modelo("modelo_forcas")
perfis, _ = ultimo_modelo("modelo_perfis")
gerais = {"mu": float(forcas.mu[0]), "mando": float(forcas.mando[0])}
restantes = spark.table(cfg.t("silver", "jogos_restantes")).toPandas()

# COMMAND ----------

probs = prob_jogos(restantes[["temporada", "rodada", "data_hora", "mandante", "visitante"]], forcas, gerais, perfis)
probs["run_id"] = cfg.run_id
probs["modelo_run_id"] = run_modelo
spark.createDataFrame(probs).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.t("gold", "prob_jogos"))
display(probs.sort_values(["rodada", "data_hora"]).head(10))
registrar_valor(dbutils, "jogos_restantes", len(probs))
