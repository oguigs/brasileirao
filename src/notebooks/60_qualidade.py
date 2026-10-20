# Databricks notebook source
# MAGIC %md
# MAGIC # 60 · Checagens de qualidade da tabela final
# MAGIC Se uma regra falhar, a tarefa falha (e o job aparece vermelho). É o gancho para mostrar
# MAGIC **notificação de falha** e **repair run** em aula: use `forcar_falha=true` para simular.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../lib"))
from brasileirao.config import Config, parametro, registrar_valor

cfg = Config.from_job(dbutils)
forcar_falha = parametro(dbutils, "forcar_falha", "false").lower() == "true"
t = spark.table(cfg.t("gold", "tabela_final")).toPandas()

regras = {
    "20 times na tabela": len(t) == 20,
    "chances de título somam 100%": abs(t.pct_titulo.sum() - 100) < 0.5,
    "chances de Z4 somam 400%": abs(t.pct_rebaixamento.sum() - 400) < 0.5,
    "jogos disputados + restantes = 38": bool(((t.jogos + t.jogos_restantes) == 38).all()),
    "melhor posição <= pior posição": bool((t.melhor_posicao <= t.pior_posicao).all()),
    "sem valores nulos": not t.isnull().any().any(),
    "falha forçada desligada": not forcar_falha,
}
for regra, ok in regras.items():
    print(("OK   " if ok else "FALHA"), regra)

falhas = [r for r, ok in regras.items() if not ok]
registrar_valor(dbutils, "regras_com_falha", len(falhas))
if falhas:
    raise AssertionError(f"Qualidade reprovada: {falhas}")
