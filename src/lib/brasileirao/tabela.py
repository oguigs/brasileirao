"""Classificação, simulação de Monte Carlo e limites matemáticos (melhor/pior posição)."""
from __future__ import annotations

import numpy as np
import pandas as pd

ZONAS = {"titulo": (1, 1), "top5": (1, 5), "z4": (17, 20)}


def classificacao(partidas: pd.DataFrame) -> pd.DataFrame:
    """Tabela a partir dos resultados. Desempate: pontos, vitórias, saldo, gols pró."""
    lados = []
    for lado, gp, gc in (("mandante", "gols_mandante", "gols_visitante"), ("visitante", "gols_visitante", "gols_mandante")):
        x = partidas[[lado, gp, gc]].rename(columns={lado: "time", gp: "gp", gc: "gc"})
        lados.append(x)
    x = pd.concat(lados)
    x["v"] = (x.gp > x.gc).astype(int)
    x["e"] = (x.gp == x.gc).astype(int)
    x["d"] = (x.gp < x.gc).astype(int)
    t = x.groupby("time").agg(jogos=("v", "size"), vitorias=("v", "sum"), empates=("e", "sum"),
                              derrotas=("d", "sum"), gols_pro=("gp", "sum"), gols_contra=("gc", "sum")).reset_index()
    t["pontos"] = 3 * t.vitorias + t.empates
    t["saldo"] = t.gols_pro - t.gols_contra
    t = t.sort_values(["pontos", "vitorias", "saldo", "gols_pro", "time"], ascending=[False, False, False, False, True])
    t.insert(0, "posicao", range(1, len(t) + 1))
    return t.reset_index(drop=True)


def simular(classif: pd.DataFrame, jogos: pd.DataFrame, n: int = 200_000, seed: int = 7):
    """Sorteia os jogos restantes n vezes. jogos: mandante, visitante, p_mandante, p_empate."""
    times = classif["time"].tolist()
    idx = {t: i for i, t in enumerate(times)}
    nt = len(times)
    rng = np.random.default_rng(seed)
    P = np.tile(classif["pontos"].to_numpy(float), (n, 1))
    V = np.tile(classif["vitorias"].to_numpy(float), (n, 1))
    for r in jogos.itertuples(index=False):
        u = rng.random(n)
        h, a = idx[r.mandante], idx[r.visitante]
        hw = u < r.p_mandante
        dr = (u >= r.p_mandante) & (u < r.p_mandante + r.p_empate)
        aw = ~(hw | dr)
        P[:, h] += 3 * hw + dr
        P[:, a] += 3 * aw + dr
        V[:, h] += hw
        V[:, a] += aw
    sg = classif["saldo"].to_numpy(float)
    chave = P * 1e6 + V * 1e3 + sg + rng.random((n, nt)) * 0.1
    ordem = np.argsort(-chave, axis=1)
    pos = np.empty_like(ordem)
    pos[np.arange(n)[:, None], ordem] = np.arange(1, nt + 1)
    linhas = []
    for t in times:
        i = idx[t]
        dist = np.bincount(pos[:, i], minlength=nt + 1)[1:] / n
        linhas.append({
            "time": t,
            "pontos_atuais": int(classif.loc[i, "pontos"]),
            "pontos_esperados": float(P[:, i].mean()),
            "pontos_p10": float(np.percentile(P[:, i], 10)),
            "pontos_p90": float(np.percentile(P[:, i], 90)),
            "posicao_media": float(pos[:, i].mean()),
            "posicao_mais_provavel": int(dist.argmax()) + 1,
            "p_titulo": float(dist[0]),
            "p_top5": float(dist[:5].sum()),
            "p_z4": float(dist[16:].sum()),
            "dist_posicoes": [float(v) for v in dist],
        })
    proj = pd.DataFrame(linhas).sort_values("posicao_media").reset_index(drop=True)
    ordenado = np.sort(P, axis=1)[:, ::-1]
    resumo = {"pontos_campeao": float(ordenado[:, 0].mean()), "pontos_17o": float(ordenado[:, 16].mean()), "simulacoes": n}
    return proj, resumo


def extremos(classif: pd.DataFrame, jogos: pd.DataFrame) -> pd.DataFrame:
    """Melhor e pior posição matematicamente possíveis, respeitando a tabela de jogos
    (dois rivais não podem vencer o mesmo jogo). Programação inteira (scipy/HiGHS).
    Desempate: pontos e depois vitórias; empate total favorece o time no melhor caso."""
    from scipy.optimize import Bounds, LinearConstraint, milp

    times = classif["time"].tolist()
    ti = {t: i for i, t in enumerate(times)}
    nt, ng = len(times), len(jogos)
    J = list(zip(jogos.mandante, jogos.visitante))
    W = 1000
    A = np.zeros((nt, 3 * ng))
    for g, (h, a) in enumerate(J):
        A[ti[h], 3 * g] += 3 * W + 1
        A[ti[h], 3 * g + 1] += W
        A[ti[a], 3 * g + 2] += 3 * W + 1
        A[ti[a], 3 * g + 1] += W
    base = (W * classif["pontos"] + classif["vitorias"]).to_numpy(float)
    M = W * 3 * 40 + 100

    def resolver(x, modo):
        nv = 3 * ng + nt
        c = np.zeros(nv)
        c[3 * ng:] = 1 if modo == "melhor" else -1
        E = np.zeros((ng, nv))
        for g in range(ng):
            E[g, 3 * g:3 * g + 3] = 1
        lb, ub = np.zeros(nv), np.ones(nv)
        for g, (h, a) in enumerate(J):
            if x in (h, a):
                vence = 3 * g if h == x else 3 * g + 2
                perde = 3 * g + 2 if h == x else 3 * g
                k = vence if modo == "melhor" else perde
                lb[k] = ub[k] = 1
        ub[3 * ng + ti[x]] = 0
        sx = base[ti[x]] + sum(A[ti[x], k] for k in range(3 * ng) if lb[k] == 1)
        R = np.zeros((nt, nv))
        R[:, :3 * ng] = A
        lo, hi = np.full(nt, -np.inf), np.full(nt, np.inf)
        for t in range(nt):
            if t == ti[x]:
                continue
            R[t, 3 * ng + t] = -M
            if modo == "melhor":
                hi[t] = sx - base[t]          # rival só fica à frente se y_t = 1
            else:
                lo[t] = sx - base[t] - M      # rival só conta atrás se z_t = 1 e alcança x
        res = milp(c, constraints=[LinearConstraint(E, 1, 1), LinearConstraint(R, lo, hi)],
                   integrality=np.ones(nv), bounds=Bounds(lb, ub))
        if not res.success:
            raise RuntimeError(f"MILP falhou para {x} ({modo}): {res.message}")
        return 1 + int(round(res.x[3 * ng:].sum()))

    linhas = []
    for x in times:
        restam = sum(1 for h, a in J if x in (h, a))
        linhas.append({"time": x, "pontos": int(classif.loc[ti[x], "pontos"]), "jogos_restantes": restam,
                       "pontos_maximos": int(classif.loc[ti[x], "pontos"]) + 3 * restam,
                       "melhor_posicao": resolver(x, "melhor"), "pior_posicao": resolver(x, "pior")})
    return pd.DataFrame(linhas)
