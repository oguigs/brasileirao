# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Listar temporadas do backfill
# MAGIC Gera a lista de temporadas que a tarefa **for each** vai percorrer.
# MAGIC A lista sai como *task value* (`temporadas`) e entra no `for_each_task.inputs` do job.

# COMMAND ----------

import json
import os
import sys

sys.path.append(os.path.abspath("../lib"))
from brasileirao.config import Config, parametro, registrar_valor

cfg = Config.from_job(dbutils)
inicio = int(parametro(dbutils, "temporada_inicio", "2012"))
fim = int(parametro(dbutils, "temporada_fim", str(cfg.temporada_atual)))

# COMMAND ----------

# só temporadas que existem de fato no arquivo de origem
disponiveis = (spark.read.option("header", True).csv(f"{cfg.pasta_historico}/BRA.csv")
               .select("Season").distinct().collect())
disponiveis = sorted(int(r.Season) for r in disponiveis if r.Season)
temporadas = [str(t) for t in disponiveis if inicio <= t <= fim]
print("temporadas:", temporadas)

# COMMAND ----------

# MAGIC %md
# MAGIC A tabela bronze é criada AQUI, antes do for each, particionada por `temporada`.
# MAGIC Assim as iterações paralelas gravam em partições diferentes e não disputam a criação da tabela.

# COMMAND ----------

from pyspark.sql import functions as F

tabela = cfg.t("bronze", "partidas_historico")
modelo = (spark.read.option("header", True).csv(f"{cfg.pasta_historico}/BRA.csv").limit(0)
          .withColumn("temporada", F.lit(0)).withColumn("_arquivo_origem", F.lit(""))
          .withColumn("_carregado_em", F.current_timestamp()))
if not spark.catalog.tableExists(tabela):
    modelo.write.format("delta").partitionBy("temporada").saveAsTable(tabela)
    print("criada:", tabela)

registrar_valor(dbutils, "temporadas", temporadas)   # lista JSON -> for_each_task.inputs
registrar_valor(dbutils, "quantidade", len(temporadas))
