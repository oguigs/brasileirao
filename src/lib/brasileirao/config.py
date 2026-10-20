"""Nomes de tabelas, volume e leitura de parâmetros, iguais em todos os notebooks."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone


def parametro(dbutils, nome: str, padrao: str = "") -> str:
    """Lê um parâmetro do job (ou o widget, quando o notebook roda à mão)."""
    try:
        dbutils.widgets.text(nome, padrao)
    except Exception:  # widget já criado
        pass
    valor = dbutils.widgets.get(nome)
    return padrao if valor in ("", None) else valor


@dataclass
class Config:
    catalogo: str
    prefixo: str
    temporada_atual: int
    run_id: str
    data_ref: datetime

    @classmethod
    def from_job(cls, dbutils):
        data_txt = parametro(dbutils, "data_ref", "")
        data_ref = datetime.fromisoformat(data_txt.replace("Z", "+00:00")) if data_txt and not data_txt.startswith("{{") \
            else datetime.now(timezone.utc)
        run_id = parametro(dbutils, "run_id", "manual")
        return cls(
            catalogo=parametro(dbutils, "catalogo", "workspace"),
            prefixo=parametro(dbutils, "prefixo", "brasileirao"),
            temporada_atual=int(parametro(dbutils, "temporada_atual", "2026")),
            run_id="manual" if run_id.startswith("{{") else run_id,
            data_ref=data_ref.astimezone(timezone.utc).replace(tzinfo=None),
        )

    # schemas (camadas do medalhão)
    @property
    def bronze(self): return f"{self.catalogo}.{self.prefixo}_bronze"
    @property
    def silver(self): return f"{self.catalogo}.{self.prefixo}_silver"
    @property
    def gold(self): return f"{self.catalogo}.{self.prefixo}_gold"
    @property
    def ops(self): return f"{self.catalogo}.{self.prefixo}_ops"

    # volume de entrada (landing zone)
    @property
    def volume(self): return f"/Volumes/{self.catalogo}/{self.prefixo}_bronze/landing"
    @property
    def pasta_historico(self): return f"{self.volume}/historico"
    @property
    def pasta_resultados(self): return f"{self.volume}/resultados"
    @property
    def pasta_calendario(self): return f"{self.volume}/calendario"
    @property
    def pasta_referencia(self): return f"{self.volume}/referencia"
    @property
    def checkpoints(self): return f"{self.volume}/_checkpoints"

    def t(self, camada: str, tabela: str) -> str:
        return f"{getattr(self, camada)}.{tabela}"


def registrar_valor(dbutils, chave: str, valor):
    """Publica um task value (texto curto) para as próximas tarefas e para condition tasks."""
    try:
        dbutils.jobs.taskValues.set(key=chave, value=valor)
    except Exception:
        print(f"[fora de um job] task value {chave} = {json.dumps(valor, ensure_ascii=False)}")
