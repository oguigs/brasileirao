# Databricks notebook source
# MAGIC %md
# MAGIC # 90 · Tarefa de demonstração
# MAGIC Notebook genérico para os jobs `padrao_*`: espera alguns segundos, imprime o que recebeu,
# MAGIC publica task values e pode falhar de propósito. Barato e rápido, ideal para ver a
# MAGIC orquestração acontecer sem esperar o processamento de verdade.
# MAGIC
# MAGIC | parâmetro | efeito |
# MAGIC |---|---|
# MAGIC | `nome` | aparece no log |
# MAGIC | `segundos` | quanto tempo a tarefa "trabalha" |
# MAGIC | `chance_falha` | 0 a 1; sorteia uma falha (para ver retries) |
# MAGIC | `falhar` | `true` falha sempre (para ver `run_if` e repair) |
# MAGIC | `valor` | publicado como task value `valor` |
# MAGIC | `lista` | lista separada por vírgula, publicada como task value `lista` (entrada de um for each) |

# COMMAND ----------

import json
import os
import random
import sys
import time

sys.path.append(os.path.abspath("../lib"))
from brasileirao.config import parametro, registrar_valor

nome = parametro(dbutils, "nome", "tarefa")
segundos = float(parametro(dbutils, "segundos", "5"))
chance_falha = float(parametro(dbutils, "chance_falha", "0"))
falhar = parametro(dbutils, "falhar", "false").lower() == "true"
valor = parametro(dbutils, "valor", "")
lista = [x.strip() for x in parametro(dbutils, "lista", "").split(",") if x.strip()]

print(f"[{nome}] começou; vai trabalhar {segundos:.0f}s")
print("parâmetros recebidos:", json.dumps({k: dbutils.widgets.get(k) for k in
      ("nome", "segundos", "chance_falha", "falhar", "valor", "lista")}, ensure_ascii=False))
time.sleep(segundos)

if falhar or random.random() < chance_falha:
    raise RuntimeError(f"[{nome}] falhou de propósito")

if valor:
    registrar_valor(dbutils, "valor", valor)
if lista:
    registrar_valor(dbutils, "lista", lista)
print(f"[{nome}] terminou")
