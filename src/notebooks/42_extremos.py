# Databricks notebook source
# MAGIC %md
# MAGIC # 42 · Limites matemáticos (melhor e pior posição possível)
# MAGIC Não usa probabilidade: resolve, para cada time, qual combinação de resultados o deixa
# MAGIC na melhor e na pior posição, sabendo que dois rivais não podem vencer o mesmo jogo.
# MAGIC Não depende do modelo, então roda **em paralelo** ao treino e à simulação.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../lib"))
from brasileirao import extremos
from brasileirao.config import Config

cfg = Config.from_job(dbutils)
classif = spark.table(cfg.t("gold", "classificacao_atual")).toPandas()
restantes = spark.table(cfg.t("silver", "jogos_restantes")).toPandas()

limites = extremos(classif, restantes)
limites["data_ref"] = cfg.data_ref
spark.createDataFrame(limites).write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(cfg.t("gold", "limites_matematicos"))
display(limites)
