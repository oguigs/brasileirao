# Databricks notebook source
# MAGIC %md
# MAGIC # 20 · Silver: partidas limpas
# MAGIC Uma linha por jogo, com nomes de times padronizados e tipos corretos.
# MAGIC
# MAGIC | modo | de onde lê | como grava |
# MAGIC |---|---|---|
# MAGIC | `full` (histórico) | bronze inteiro + todos os resultados recebidos | `overwrite` da tabela |
# MAGIC | `incremental` | só o lote validado agora (`silver.resultados_lote`) | `MERGE` (insere ou corrige) |

# COMMAND ----------

import os
import sys

from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../lib"))
from brasileirao.config import Config, parametro, registrar_valor

cfg = Config.from_job(dbutils)
modo = parametro(dbutils, "modo", "full")
destino = cfg.t("silver", "partidas")

alias = (spark.read.option("header", True).csv(f"{cfg.pasta_referencia}/dim_times.csv"))
alias.write.mode("overwrite").saveAsTable(cfg.t("silver", "dim_times"))


def padronizar(df, col):
    """Troca o nome do time pelo nome padrão da dim_times (ou mantém o original)."""
    a = alias.select(F.col("alias").alias(f"_a_{col}"), F.col("time").alias(f"_t_{col}"))
    return (df.join(a, df[col] == a[f"_a_{col}"], "left")
              .withColumn(col, F.coalesce(F.col(f"_t_{col}"), F.col(col)))
              .drop(f"_a_{col}", f"_t_{col}"))


COLUNAS = ["temporada", "data_hora", "mandante", "visitante", "gols_mandante", "gols_visitante",
           "odd_mandante", "odd_empate", "odd_visitante", "origem"]

# COMMAND ----------

if modo == "full":
    hist = (spark.table(cfg.t("bronze", "partidas_historico"))
            .filter(F.col("HG").isNotNull())
            .select(
                F.col("temporada").cast("int").alias("temporada"),
                F.to_timestamp(F.concat_ws(" ", F.trim("Date"), F.coalesce(F.trim("Time"), F.lit("00:00"))), "dd/MM/yyyy HH:mm").alias("data_hora"),
                F.trim("Home").alias("mandante"), F.trim("Away").alias("visitante"),
                F.col("HG").cast("int").alias("gols_mandante"), F.col("AG").cast("int").alias("gols_visitante"),
                F.col("AvgCH").cast("double").alias("odd_mandante"), F.col("AvgCD").cast("double").alias("odd_empate"),
                F.col("AvgCA").cast("double").alias("odd_visitante"),
                F.lit("historico").alias("origem")))
    partes = [hist]
    recebidos = cfg.t("silver", "resultados_validados")
    if spark.catalog.tableExists(recebidos):  # resultados que chegaram depois do arquivo histórico
        partes.append(spark.table(recebidos).select(*COLUNAS))
    todas = partes[0]
    for p in partes[1:]:
        todas = todas.unionByName(p)
    todas = padronizar(padronizar(todas, "mandante"), "visitante")
    # o mesmo jogo pode vir das duas origens: fica o mais recente (resultado recebido > histórico)
    from pyspark.sql.window import Window
    janela = Window.partitionBy("temporada", "mandante", "visitante").orderBy(
        F.when(F.col("origem") == "historico", 1).otherwise(0), F.col("data_hora").desc())
    todas = (todas.withColumn("_n", F.row_number().over(janela))
                  .filter("_n = 1").drop("_n"))
    todas.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(destino)
    n = spark.table(destino).count()
    print(f"{destino}: {n} jogos (carga completa)")
    registrar_valor(dbutils, "jogos_silver", n)

# COMMAND ----------

if modo == "incremental":
    lote = cfg.t("silver", "resultados_lote")
    novos = padronizar(padronizar(spark.table(lote).select(*COLUNAS), "mandante"), "visitante")
    novos.createOrReplaceTempView("lote_atual")
    spark.sql(f"""
        MERGE INTO {destino} AS s
        USING lote_atual AS n
          ON s.temporada = n.temporada AND s.mandante = n.mandante AND s.visitante = n.visitante
        WHEN MATCHED AND (s.gols_mandante <> n.gols_mandante OR s.gols_visitante <> n.gols_visitante)
          THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    # guarda o que foi recebido para que uma futura carga completa (modo full) não perca esses jogos
    alvo = cfg.t("silver", "resultados_validados")
    if spark.catalog.tableExists(alvo):
        novos.write.mode("append").saveAsTable(alvo)
    else:
        novos.write.saveAsTable(alvo)
    m = spark.sql(f"DESCRIBE HISTORY {destino} LIMIT 1").select("operationMetrics").first()[0]
    print("MERGE:", {k: m.get(k) for k in ("numTargetRowsInserted", "numTargetRowsUpdated")})
    registrar_valor(dbutils, "jogos_inseridos", int(m.get("numTargetRowsInserted", 0)))