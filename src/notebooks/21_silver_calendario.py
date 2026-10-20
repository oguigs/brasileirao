# Databricks notebook source
# MAGIC %md
# MAGIC # 21 · Silver: calendário de jogos
# MAGIC Carrega a tabela de jogos da temporada. Os **jogos restantes** são os do calendário
# MAGIC que ainda não aparecem em `silver.partidas` (calculado na hora, nunca guardado à mão).

# COMMAND ----------

import os
import sys

from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../lib"))
from brasileirao.config import Config, registrar_valor

cfg = Config.from_job(dbutils)

cal = (spark.read.option("header", True).csv(f"{cfg.pasta_calendario}/")
       .select(F.col("temporada").cast("int"), F.col("rodada").cast("int"),
               F.to_timestamp("data_hora_utc").alias("data_hora"), "mandante", "visitante"))
cal.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.t("silver", "calendario"))

# view usada por várias tarefas: o que falta jogar
spark.sql(f"""
CREATE OR REPLACE VIEW {cfg.t('silver', 'jogos_restantes')} AS
SELECT c.* FROM {cfg.t('silver', 'calendario')} c
LEFT ANTI JOIN {cfg.t('silver', 'partidas')} p
  ON p.temporada = c.temporada AND p.mandante = c.mandante AND p.visitante = c.visitante
""")
n = spark.table(cfg.t("silver", "jogos_restantes")).count()
print("jogos restantes:", n)
registrar_valor(dbutils, "jogos_restantes", n)
