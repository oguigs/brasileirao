# Databricks notebook source
# MAGIC %md
# MAGIC # 70 · Bronze: ingerir arquivos de resultados que chegaram
# MAGIC **Auto Loader** com `availableNow`: lê só os arquivos novos da pasta `landing/resultados`
# MAGIC (o checkpoint lembra o que já foi lido) e para. É o par natural do **gatilho de chegada de arquivo**.

# COMMAND ----------

import os
import sys

from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../lib"))
from brasileirao.config import Config, parametro, registrar_valor

cfg = Config.from_job(dbutils)
destino = cfg.t("bronze", "resultados_recebidos")
local_gatilho = parametro(dbutils, "local_gatilho", "")  # {{job.trigger.file_arrival.location}}, se houver
print("disparado por arquivo em:", local_gatilho or "(execução manual/agendada)")

ESQUEMA = ("temporada STRING, rodada STRING, data_hora_utc STRING, mandante STRING, visitante STRING, "
           "gols_mandante STRING, gols_visitante STRING")

# COMMAND ----------

antes = spark.table(destino).count() if spark.catalog.tableExists(destino) else 0
consulta = (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", True)
            .option("cloudFiles.schemaEvolutionMode", "rescue")  # coluna inesperada vai para _rescued_data
            .schema(ESQUEMA)
            .load(cfg.pasta_resultados)
            .withColumn("_arquivo", F.col("_metadata.file_path"))
            .withColumn("_ingerido_em", F.current_timestamp())
            .writeStream
            .option("checkpointLocation", f"{cfg.checkpoints}/resultados")
            .trigger(availableNow=True)
            .toTable(destino))
consulta.awaitTermination()

linhas = spark.table(destino).count() - antes
print(f"{linhas} linhas novas em {destino}")
registrar_valor(dbutils, "linhas_lidas", linhas)
