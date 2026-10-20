# Databricks notebook source
# MAGIC %md
# MAGIC # 22 · Gold: classificação atual
# MAGIC Tabela de pontos da temporada, calculada só a partir dos resultados em `silver.partidas`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../lib"))
from brasileirao import classificacao
from brasileirao.config import Config, registrar_valor

cfg = Config.from_job(dbutils)

partidas = spark.table(cfg.t("silver", "partidas")).filter(f"temporada = {cfg.temporada_atual}").toPandas()
tabela = classificacao(partidas)
tabela["atualizado_em"] = cfg.data_ref

spark.createDataFrame(tabela).write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(cfg.t("gold", "classificacao_atual"))
display(tabela)

lider = tabela.iloc[0]
registrar_valor(dbutils, "lider", f"{lider.time} ({int(lider.pontos)} pts)")
registrar_valor(dbutils, "jogos_disputados", int(tabela.jogos.sum() // 2))
