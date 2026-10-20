# Databricks notebook source
# MAGIC %md
# MAGIC # 72 · Registrar a execução
# MAGIC Grava uma linha em `ops.execucoes`. No job, esta tarefa usa `run_if: ALL_DONE`:
# MAGIC roda mesmo se uma tarefa anterior falhar, como o `trigger_rule="all_done"` do Airflow.
# MAGIC Lê os task values com `default`, porque a tarefa de origem pode ter falhado ou sido pulada.

# COMMAND ----------

import json
import os
import sys
from datetime import datetime, timezone

sys.path.append(os.path.abspath("../lib"))
from brasileirao.config import Config, parametro

cfg = Config.from_job(dbutils)

# lê task values de outras tarefas COM valor padrão: se a tarefa falhou ou foi pulada, não quebra aqui
detalhes = {}
for item in [x for x in parametro(dbutils, "ler_valores", "").split(",") if x.strip()]:
    tarefa, chave = item.strip().split(".")
    try:
        detalhes[item] = dbutils.jobs.taskValues.get(taskKey=tarefa, key=chave, default=None, debugValue=None)
    except Exception:
        detalhes[item] = None

linha = {
    "job_run_id": cfg.run_id,
    "job_nome": parametro(dbutils, "job_nome", "manual"),
    "gatilho": parametro(dbutils, "gatilho", "manual"),          # {{job.trigger.type}}
    "etapa": parametro(dbutils, "etapa", "fim"),
    "status": parametro(dbutils, "status", "concluido"),
    "detalhes": json.dumps(detalhes, ensure_ascii=False, default=str),
    "registrado_em": datetime.now(timezone.utc).replace(tzinfo=None),
}
spark.createDataFrame([linha]).write.mode("append").saveAsTable(cfg.t("ops", "execucoes"))
print(json.dumps(linha, default=str, ensure_ascii=False, indent=1))
