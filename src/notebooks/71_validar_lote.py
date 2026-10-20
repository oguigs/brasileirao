# Databricks notebook source
# MAGIC %md
# MAGIC # 71 · Validar o lote recebido
# MAGIC Separa o que chegou em três grupos e publica as contagens como *task values*:
# MAGIC
# MAGIC | grupo | vai para | task value |
# MAGIC |---|---|---|
# MAGIC | novo ou corrigido, válido | `silver.resultados_lote` | `n_novos` |
# MAGIC | inválido (time desconhecido, gol negativo, campo vazio) | `bronze.quarentena` | `n_invalidos` |
# MAGIC | igual ao que já está na silver | descartado | — |
# MAGIC
# MAGIC A próxima tarefa é uma **condition task**: se `n_novos > 0` segue o pipeline, senão só registra.

# COMMAND ----------

import os
import sys

from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../lib"))
from brasileirao.config import Config, registrar_valor

cfg = Config.from_job(dbutils)
bronze = cfg.t("bronze", "resultados_recebidos")
controle = cfg.t("ops", "controle_lotes")

spark.sql(f"CREATE TABLE IF NOT EXISTS {controle} (ultimo_ingerido_em TIMESTAMP, atualizado_em TIMESTAMP)")
marca = spark.table(controle).agg(F.max("ultimo_ingerido_em")).first()[0]
lote = spark.table(bronze)
if marca is not None:
    lote = lote.filter(F.col("_ingerido_em") > F.lit(marca))   # só o que entrou depois do último lote

# COMMAND ----------

times = spark.table(cfg.t("silver", "dim_times")) if spark.catalog.tableExists(cfg.t("silver", "dim_times")) \
    else spark.read.option("header", True).csv(f"{cfg.pasta_referencia}/dim_times.csv")
conhecidos = [r.alias for r in times.select("alias").collect()]

tipado = (lote.select(
    F.col("temporada").cast("int").alias("temporada"),
    F.to_timestamp("data_hora_utc").alias("data_hora"),
    F.trim("mandante").alias("mandante"), F.trim("visitante").alias("visitante"),
    F.col("gols_mandante").cast("int").alias("gols_mandante"),
    F.col("gols_visitante").cast("int").alias("gols_visitante"),
    F.lit(None).cast("double").alias("odd_mandante"), F.lit(None).cast("double").alias("odd_empate"),
    F.lit(None).cast("double").alias("odd_visitante"),
    F.col("_arquivo").alias("origem"), "_ingerido_em"))

problema = (F.when(F.col("temporada").isNull() | F.col("data_hora").isNull(), "temporada ou data vazia")
             .when(F.col("gols_mandante").isNull() | F.col("gols_visitante").isNull(), "placar vazio")
             .when((F.col("gols_mandante") < 0) | (F.col("gols_visitante") < 0), "gol negativo")
             .when(~F.col("mandante").isin(conhecidos), "mandante desconhecido")
             .when(~F.col("visitante").isin(conhecidos), "visitante desconhecido"))
avaliado = tipado.withColumn("problema", problema)  # (serverless não usa .cache(); o lote é pequeno)

invalidos = avaliado.filter("problema IS NOT NULL")
validos = avaliado.filter("problema IS NULL").drop("problema", "_ingerido_em").dropDuplicates(
    ["temporada", "mandante", "visitante", "gols_mandante", "gols_visitante"])

# descarta o que já está igualzinho na silver (arquivo reenviado, por exemplo)
silver = cfg.t("silver", "partidas")
if spark.catalog.tableExists(silver):
    a = times.select(F.col("alias"), F.col("time"))
    validos_norm = (validos.join(a.withColumnRenamed("alias", "am").withColumnRenamed("time", "tm"), F.col("mandante") == F.col("am"), "left")
                           .join(a.withColumnRenamed("alias", "av").withColumnRenamed("time", "tv"), F.col("visitante") == F.col("av"), "left"))
    ja = spark.table(silver).select("temporada", F.col("mandante").alias("tm"), F.col("visitante").alias("tv"),
                                    "gols_mandante", "gols_visitante")
    validos = (validos_norm.join(ja, ["temporada", "tm", "tv", "gols_mandante", "gols_visitante"], "left_anti")
                           .drop("am", "av", "tm", "tv"))

n_invalidos = invalidos.count()
if n_invalidos:
    invalidos.write.mode("append").option("mergeSchema", "true").saveAsTable(cfg.t("bronze", "quarentena"))
    display(invalidos)

validos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.t("silver", "resultados_lote"))
n_novos = spark.table(cfg.t("silver", "resultados_lote")).count()

# avança a marca d'água: o próximo lote começa depois deste
fim_lote = avaliado.agg(F.max("_ingerido_em")).first()[0]
if fim_lote is not None:
    spark.sql(f"INSERT INTO {controle} VALUES (timestamp'{fim_lote}', current_timestamp())")

print(f"novos/corrigidos: {n_novos} · inválidos: {n_invalidos}")
registrar_valor(dbutils, "n_novos", n_novos)
registrar_valor(dbutils, "n_invalidos", n_invalidos)
