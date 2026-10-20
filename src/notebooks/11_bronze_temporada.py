# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · Bronze: carregar UMA temporada do histórico
# MAGIC Cada iteração do **for each** roda este notebook com um valor de `temporada`.
# MAGIC
# MAGIC Idempotência: o `replaceWhere` troca só as linhas daquela temporada.
# MAGIC Rodar de novo (ou fazer *repair* de uma iteração que falhou) não duplica nada.

# COMMAND ----------

import os
import sys

from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../lib"))
from brasileirao.config import Config, parametro

cfg = Config.from_job(dbutils)
temporada = int(parametro(dbutils, "temporada", "2025"))
tabela = cfg.t("bronze", "partidas_historico")

# COMMAND ----------

bruto = (spark.read.option("header", True).option("inferSchema", False)
         .csv(f"{cfg.pasta_historico}/BRA.csv")
         .filter(F.col("Season") == str(temporada))
         .withColumn("temporada", F.lit(temporada))
         .withColumn("_arquivo_origem", F.col("_metadata.file_path"))
         .withColumn("_carregado_em", F.current_timestamp()))

linhas = bruto.count()
assert linhas > 0, f"Nenhuma linha para a temporada {temporada}"

(bruto.write.format("delta").mode("overwrite")
 .option("replaceWhere", f"temporada = {temporada}")
 .option("mergeSchema", "true")
 .saveAsTable(tabela))

print(f"{tabela}: temporada {temporada} gravada com {linhas} linhas")
