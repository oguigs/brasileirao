"""Biblioteca do pipeline do Brasileirão: modelo de jogos, tabela, simulação e limites."""
from .modelo import PARAMS, ajustar_forcas, ajustar_perfis, backtest, janela_treino, prob_jogos, promovidos, rps
from .tabela import classificacao, extremos, simular

__all__ = ["PARAMS", "ajustar_forcas", "ajustar_perfis", "backtest", "janela_treino", "prob_jogos",
           "promovidos", "rps", "classificacao", "extremos", "simular"]