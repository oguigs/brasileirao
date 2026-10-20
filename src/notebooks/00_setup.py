# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup do ambiente
# MAGIC Cria os schemas do medalhão, o volume de entrada (landing) e copia os arquivos do projeto para ele.
# MAGIC Pode rodar quantas vezes quiser: tudo é `IF NOT EXISTS` (idempotente).

# COMMAND ----------

import os
import shutil
import sys

sys.path.append(os.path.abspath("../lib"))
from brasileirao.config import Config, parametro

cfg = Config.from_job(dbutils)
copiar_exemplos = parametro(dbutils, "copiar_exemplos", "false").lower() == "true"
print(cfg)

# COMMAND ----------

for schema in (cfg.bronze, cfg.silver, cfg.gold, cfg.ops):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.bronze}.landing COMMENT 'Arquivos que chegam: histórico, calendário e resultados de cada rodada'")

for pasta in (cfg.pasta_historico, cfg.pasta_resultados, cfg.pasta_calendario, cfg.pasta_referencia, cfg.checkpoints):
    os.makedirs(pasta, exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Os arquivos `dados/` ficam na pasta do projeto no workspace (Git folder).
# MAGIC Aqui eles são copiados para o volume, que é de onde os jobs leem (como chegariam de um sistema de origem).

# COMMAND ----------

origem = os.path.abspath("../../dados")
copias = {
    "historico/BRA.csv": cfg.pasta_historico,
    "calendario/calendario_restante_2026.csv": cfg.pasta_calendario,
    "referencia/dim_times.csv": cfg.pasta_referencia,
}
for arquivo, destino in copias.items():
    shutil.copy(os.path.join(origem, arquivo), destino)
    print("copiado:", arquivo, "->", destino)

if copiar_exemplos:  # na aula, os alunos soltam esses arquivos à mão para ver o gatilho disparar
    for nome in sorted(os.listdir(os.path.join(origem, "resultados_exemplo"))):
        shutil.copy(os.path.join(origem, "resultados_exemplo", nome), cfg.pasta_resultados)
        print("exemplo copiado:", nome)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.t('ops', 'execucoes')} (
  job_run_id STRING, job_nome STRING, gatilho STRING, etapa STRING, status STRING,
  detalhes STRING, registrado_em TIMESTAMP)
COMMENT 'Log de execuções dos jobs do Brasileirão (uma linha por execução)'
""")
display(spark.sql(f"SHOW TABLES IN {cfg.ops}"))
