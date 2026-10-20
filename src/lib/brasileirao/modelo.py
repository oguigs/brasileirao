"""Modelo de probabilidades por jogo (só pandas/numpy/scipy, sem Spark).

Duas partes, misturadas 75% / 25%:
  A) modelo de gols (Poisson): ataque e defesa de cada time + mando de campo,
     jogos antigos com peso menor (meia-vida em dias) e encolhimento para a média.
  B) perfil casa/fora no estilo da UFMG: taxas de V/E/D em casa e fora,
     ajustadas pela força do adversário e encolhidas para a média da liga.

Entrada padrão (silver): colunas temporada, data_hora, mandante, visitante,
gols_mandante, gols_visitante.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.stats import poisson

PARAMS = {
    "meia_vida_dias": 365,          # um jogo de 1 ano atrás vale metade de um jogo de hoje
    "sigma": 0.6,                   # força do encolhimento (menor = puxa mais para a média)
    "prior_promovidos": 0.3,        # times que subiram começam 0,3 abaixo da média
    "meia_vida_perfil_dias": 730,
    "k_perfil": 6,                  # "jogos fictícios" da média da liga no perfil casa/fora
    "peso_gols": 0.75,              # mistura: 75% modelo de gols + 25% perfil
    "temporadas_treino": 5,         # usa a temporada atual + 5 anteriores
}
MAX_GOLS = 11


def _resultado(gm, gv):
    return np.where(gm > gv, 0, np.where(gm == gv, 1, 2))  # 0 mandante, 1 empate, 2 visitante


def janela_treino(partidas: pd.DataFrame, temporada: int, data_ref, params=PARAMS) -> pd.DataFrame:
    """Jogos usados no ajuste: anteriores a data_ref, das últimas N temporadas."""
    data_ref = pd.Timestamp(data_ref)
    m = (partidas["data_hora"] < data_ref) & (partidas["temporada"] >= temporada - params["temporadas_treino"])
    return partidas.loc[m].copy()


def promovidos(partidas: pd.DataFrame, temporada: int) -> set:
    atual = set(partidas.loc[partidas.temporada == temporada, "mandante"])
    anterior = set(partidas.loc[partidas.temporada == temporada - 1, "mandante"])
    return atual - anterior if anterior else set()


def ajustar_forcas(treino: pd.DataFrame, data_ref, times_promovidos=(), params=PARAMS):
    """Ajusta ataque/defesa por máxima verossimilhança ponderada. Devolve (DataFrame, gerais)."""
    times = sorted(set(treino.mandante) | set(treino.visitante))
    idx = {t: i for i, t in enumerate(times)}
    n = len(times)
    idade = (pd.Timestamp(data_ref) - treino["data_hora"]).dt.total_seconds().to_numpy() / 86400
    w = 0.5 ** (idade / params["meia_vida_dias"])
    hi = treino["mandante"].map(idx).to_numpy()
    ai = treino["visitante"].map(idx).to_numpy()
    hg = treino["gols_mandante"].to_numpy(float)
    ag = treino["gols_visitante"].to_numpy(float)
    prior = np.zeros(n)
    for t in times_promovidos:
        if t in idx:
            prior[idx[t]] = -params["prior_promovidos"]
    s2 = params["sigma"] ** 2

    def nll(x):
        mu, ha, att, de = x[0], x[1], x[2:2 + n], x[2 + n:]
        lh = mu + ha + att[hi] - de[ai]
        la = mu + att[ai] - de[hi]
        Lh, La = np.exp(lh), np.exp(la)
        f = np.sum(w * (Lh - hg * lh + La - ag * la)) + (np.sum((att - prior) ** 2) + np.sum((de - prior) ** 2)) / (2 * s2)
        rh, ra = w * (Lh - hg), w * (La - ag)
        g = np.zeros_like(x)
        g[0], g[1] = rh.sum() + ra.sum(), rh.sum()
        g[2:2 + n] = np.bincount(hi, rh, n) + np.bincount(ai, ra, n) + (att - prior) / s2
        g[2 + n:] = -np.bincount(ai, rh, n) - np.bincount(hi, ra, n) + (de - prior) / s2
        return f, g

    x0 = np.zeros(2 + 2 * n)
    x0[0], x0[1] = 0.1, 0.3
    x = minimize(nll, x0, jac=True, method="L-BFGS-B").x
    forcas = pd.DataFrame({"time": times, "ataque": x[2:2 + n], "defesa": x[2 + n:]})
    return forcas, {"mu": float(x[0]), "mando": float(x[1])}


def ajustar_perfis(treino: pd.DataFrame, data_ref, params=PARAMS) -> pd.DataFrame:
    """Perfil V/E/D em casa e fora (estilo UFMG), ponderado pela força do adversário."""
    times = sorted(set(treino.mandante) | set(treino.visitante))
    idx = {t: i for i, t in enumerate(times)}
    n = len(times)
    idade = (pd.Timestamp(data_ref) - treino["data_hora"]).dt.total_seconds().to_numpy() / 86400
    w = 0.5 ** (idade / params["meia_vida_perfil_dias"])
    hi = treino["mandante"].map(idx).to_numpy()
    ai = treino["visitante"].map(idx).to_numpy()
    res = _resultado(treino["gols_mandante"].to_numpy(), treino["gols_visitante"].to_numpy())
    pts_h, pts_a = np.choose(res, [3, 1, 0]), np.choose(res, [0, 1, 3])
    pts = np.bincount(hi, w * pts_h, n) + np.bincount(ai, w * pts_a, n)
    jogos = np.bincount(hi, w, n) + np.bincount(ai, w, n)
    forca = (pts + 1.38 * 3) / (jogos + 3)
    forca = forca / np.average(forca, weights=jogos + 1e-9)
    wh = w * np.where(res < 2, forca[ai], 1 / forca[ai])
    wa = w * np.where(res > 0, forca[hi], 1 / forca[hi])
    H, A = np.zeros((n, 3)), np.zeros((n, 3))
    for r in range(3):
        H[:, r] = np.bincount(hi[res == r], wh[res == r], n)
        A[:, 2 - r] = np.bincount(ai[res == r], wa[res == r], n)
    liga_casa = np.bincount(res, w, 3) / w.sum()
    liga_fora = liga_casa[::-1]
    k = params["k_perfil"]
    H = (H + k * liga_casa) / (H.sum(1, keepdims=True) + k)
    A = (A + k * liga_fora) / (A.sum(1, keepdims=True) + k)
    return pd.DataFrame({"time": times,
                         "casa_v": H[:, 0], "casa_e": H[:, 1], "casa_d": H[:, 2],
                         "fora_v": A[:, 0], "fora_e": A[:, 1], "fora_d": A[:, 2]})


def _wdl_poisson(lh, la, gm=0, gv=0, frac=1.0):
    g = np.arange(MAX_GOLS)
    M = np.outer(poisson.pmf(g, lh * frac), poisson.pmf(g, la * frac))
    k = np.subtract.outer(g, g) + (gm - gv)
    v = np.array([M[k > 0].sum(), M[k == 0].sum(), M[k < 0].sum()])
    return v / v.sum()


def fracao_restante(minuto: int) -> float:
    """Parte dos gols esperados que ainda falta: ~45% saem no 1º tempo, ~55% no 2º."""
    if minuto <= 45:
        return 1 - 0.45 * minuto / 45
    return max(0.0, 0.55 * (90 - minuto) / 45)


def prob_jogos(jogos: pd.DataFrame, forcas: pd.DataFrame, gerais: dict, perfis: pd.DataFrame,
               params=PARAMS) -> pd.DataFrame:
    """Para cada jogo (mandante, visitante[, gols_mandante, gols_visitante, minuto]) calcula P(V/E/D)."""
    F = forcas.set_index("time")
    P = perfis.set_index("time")
    media = F[["ataque", "defesa"]].mean()
    out = []
    for r in jogos.itertuples(index=False):
        m, v = r.mandante, r.visitante
        am, dm = (F.loc[m, "ataque"], F.loc[m, "defesa"]) if m in F.index else (media.ataque, media.defesa)
        av, dv = (F.loc[v, "ataque"], F.loc[v, "defesa"]) if v in F.index else (media.ataque, media.defesa)
        lh = np.exp(gerais["mu"] + gerais["mando"] + am - dv)
        la = np.exp(gerais["mu"] + av - dm)
        minuto = getattr(r, "minuto", None)
        if minuto is not None and not pd.isna(minuto):
            p = _wdl_poisson(lh, la, int(r.gols_mandante), int(r.gols_visitante), fracao_restante(int(minuto)))
        else:
            pa = _wdl_poisson(lh, la)
            if m in P.index and v in P.index:
                ph, pv = P.loc[m], P.loc[v]
                pb = np.array([(ph.casa_v + pv.fora_d) / 2, (ph.casa_e + pv.fora_e) / 2, (ph.casa_d + pv.fora_v) / 2])
                pb = pb / pb.sum()
            else:
                pb = pa
            p = params["peso_gols"] * pa + (1 - params["peso_gols"]) * pb
        out.append({**r._asdict(), "lambda_mandante": float(lh), "lambda_visitante": float(la),
                    "p_mandante": float(p[0]), "p_empate": float(p[1]), "p_visitante": float(p[2])})
    return pd.DataFrame(out)


def rps(p: np.ndarray, resultado: np.ndarray) -> float:
    """Ranked Probability Score (menor é melhor) para V/E/D."""
    y = np.eye(3)[resultado]
    return float(((np.cumsum(p, 1) - np.cumsum(y, 1))[:, :2] ** 2).sum(1).mean() / 2)


def backtest(partidas: pd.DataFrame, temporadas, params=PARAMS) -> pd.DataFrame:
    """Walk-forward semanal: cada semana é prevista só com jogos anteriores a ela."""
    teste = partidas[partidas.temporada.isin(temporadas)].copy()
    teste["semana"] = teste["data_hora"].dt.to_period("W-MON")
    linhas = []
    for semana, jogos in teste.groupby("semana"):
        t0 = jogos["data_hora"].min()
        temporada = int(jogos["temporada"].iloc[0])
        treino = janela_treino(partidas, temporada, t0, params)
        if len(treino) < 20:
            continue
        forcas, gerais = ajustar_forcas(treino, t0, promovidos(partidas, temporada), params)
        perfis = ajustar_perfis(treino, t0, params)
        prev = prob_jogos(jogos[["mandante", "visitante"]], forcas, gerais, perfis, params)
        prev["resultado"] = _resultado(jogos["gols_mandante"].to_numpy(), jogos["gols_visitante"].to_numpy())
        prev["temporada"] = temporada
        for c in ("mercado_mandante", "mercado_empate", "mercado_visitante"):
            if c in jogos:
                prev[c] = jogos[c].to_numpy()
        linhas.append(prev)
    return pd.concat(linhas, ignore_index=True)
