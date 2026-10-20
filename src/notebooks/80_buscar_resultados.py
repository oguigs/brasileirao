# Databricks notebook source
# MAGIC %md
# MAGIC # 80 · Buscar resultados novos na internet
# MAGIC Baixa o CSV público da temporada (football-data.co.uk), separa os jogos que ainda não estão na
# MAGIC silver e grava um arquivo na pasta `landing/resultados`. **Não chama o pipeline**: o arquivo novo
# MAGIC dispara o job incremental pelo gatilho de chegada de arquivo (acoplamento por dados).
# MAGIC
# MAGIC Na Free Edition a internet de saída é restrita a alguns domínios. Se o site estiver bloqueado,
# MAGIC a tarefa termina com `status = sem_acesso` (ou falha, se `falhar_sem_acesso=true`).

# COMMAND ----------

import io
import os
import sys
import urllib.request
from datetime import datetime, timezone

import pandas as pd

sys.path.append(os.path.abspath("../lib"))
from brasileirao.config import Config, parametro, registrar_valor

cfg = Config.from_job(dbutils)
url = parametro(dbutils, "url_fonte", "https://www.football-data.co.uk/new/BRA.csv")
falhar = parametro(dbutils, "falhar_sem_acesso", "false").lower() == "true"

# COMMAND ----------

try:
    with urllib.request.urlopen(url, timeout=30) as resp:
        bruto = pd.read_csv(io.BytesIO(resp.read()), encoding="utf-8-sig")
except Exception as erro:  # domínio bloqueado, site fora do ar, timeout
    print(f"Não consegui baixar {url}: {erro}")
    registrar_valor(dbutils, "status", "sem_acesso")
    registrar_valor(dbutils, "n_baixados", 0)
    if falhar:
        raise
    dbutils.notebook.exit("sem_acesso")

# COMMAND ----------

bruto = bruto[(bruto["Season"] == cfg.temporada_atual) & bruto["HG"].notna()]
novos = pd.DataFrame({
    "temporada": bruto["Season"].astype(int),
    "rodada": "",
    "data_hora_utc": pd.to_datetime(bruto["Date"] + " " + bruto["Time"].fillna("00:00"), format="%d/%m/%Y %H:%M")
                       .dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "mandante": bruto["Home"], "visitante": bruto["Away"],
    "gols_mandante": bruto["HG"].astype(int), "gols_visitante": bruto["AG"].astype(int),
})

# só o que a silver ainda não tem (comparação pelos nomes padronizados)
alias = spark.table(cfg.t("silver", "dim_times")).toPandas().set_index("alias")["time"]
ja = spark.table(cfg.t("silver", "partidas")).filter(f"temporada = {cfg.temporada_atual}") \
    .select("mandante", "visitante").toPandas()
ja = set(zip(ja.mandante, ja.visitante))
chave = list(zip(novos.mandante.map(lambda x: alias.get(x, x)), novos.visitante.map(lambda x: alias.get(x, x))))
novos = novos[[k not in ja for k in chave]]

if len(novos):
    arquivo = f"{cfg.pasta_resultados}/busca_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}.csv"
    novos.to_csv(arquivo, index=False)  # o arquivo novo dispara o job incremental
    print(f"{len(novos)} jogos novos gravados em {arquivo}")
else:
    print("Nenhum jogo novo na fonte.")
registrar_valor(dbutils, "status", "ok")
registrar_valor(dbutils, "n_baixados", len(novos))
