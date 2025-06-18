import pandas as pd
import numpy as np

META_IDS = ['1', '2A', '2B', '2C', '2ANT', '4A', '4B', '6', '7A', '7B', '8A', '8B', '8', '10A', '10B', '10']

def calcular_metas(df: pd.DataFrame, col_julgados: str, col_distm_casos_novos: str,
                   col_dessobrestados: str | None, col_suspensos: str, multiplicador: float) -> float | None:
    """
    Calcula o valor de uma meta específica com base nas colunas fornecidas.
    Valores não numéricos ou ausentes nas colunas são tratados como 0 para a soma.
    """
    colunas_necessarias = [col_julgados, col_distm_casos_novos, col_suspensos]
    if col_dessobrestados:
        colunas_necessarias.append(col_dessobrestados)

    for col in colunas_necessarias:
        if col not in df.columns:
            return None

    for col in colunas_necessarias:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    try:
        if col_dessobrestados:
            denominador = (df[col_distm_casos_novos].sum() +
                           df[col_dessobrestados].sum() -
                           df[col_suspensos].sum())
        else:
            denominador = (df[col_distm_casos_novos].sum() -
                           df[col_suspensos].sum())

        if denominador != 0:
            return (df[col_julgados].sum() * multiplicador) / denominador
        else:
            return None
    except Exception:
        return None

def metas_justica_estadual(df_estadual: pd.DataFrame) -> dict:
    """Calcula todas as metas para a Justiça Estadual."""
    if df_estadual.empty: return {}
    return {
        'Meta 1': calcular_metas(df_estadual, 'julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025', 100.0),
        'Meta 2A': calcular_metas(df_estadual, 'julgm2_a', 'distm2_a', None, 'suspm2_a', (1000 / 8.0)),
        'Meta 2B': calcular_metas(df_estadual, 'julgm2_b', 'distm2_b', None, 'suspm2_b', (1000 / 9.0)),
        'Meta 2C': calcular_metas(df_estadual, 'julgm2_c', 'distm2_c', None, 'suspm2_c', (1000 / 9.5)),
        'Meta 2ANT': calcular_metas(df_estadual, 'julgm2_ant', 'distm2_ant', None, 'suspm2_ant', 100.0),
        'Meta 4A': calcular_metas(df_estadual, 'julgm4_a', 'distm4_a', None, 'suspm4_a', (1000 / 6.5)),
        'Meta 4B': calcular_metas(df_estadual, 'julgm4_b', 'distm4_b', None, 'suspm4_b', 100.0),
        'Meta 6': calcular_metas(df_estadual, 'julgm6', 'distm6', None, 'suspm6', 100.0),
        'Meta 7A': calcular_metas(df_estadual, 'julgm7_a', 'distm7_a', None, 'suspm7_a', (1000 / 5.0)),
        'Meta 7B': calcular_metas(df_estadual, 'julgm7_b', 'distm7_b', None, 'suspm7_b', (1000 / 5.0)),
        'Meta 8A': calcular_metas(df_estadual, 'julgm8_a', 'distm8_a', None, 'suspm8_a', (1000 / 7.5)),
        'Meta 8B': calcular_metas(df_estadual, 'julgm8_b', 'distm8_b', None, 'suspm8_b', (1000 / 9.0)),
        'Meta 10A': calcular_metas(df_estadual, 'julgm10_a', 'distm10_a', None, 'suspm10_a', (1000 / 9.0)),
        'Meta 10B': calcular_metas(df_estadual, 'julgm10_b', 'distm10_b', None, 'suspm10_b', (1000 / 10.0))
    }

def metas_justica_trabalho(df_trabalho: pd.DataFrame) -> dict:
    """Calcula todas as metas para a Justiça do Trabalho."""
    if df_trabalho.empty: return {}
    return {
        'Meta 1': calcular_metas(df_trabalho, 'julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025', 100.0),
        'Meta 2A': calcular_metas(df_trabalho, 'julgm2_a', 'distm2_a', None, 'suspm2_a', (1000 / 9.4)),
        'Meta 2ANT': calcular_metas(df_trabalho, 'julgm2_ant', 'distm2_ant', None, 'suspm2_ant', 100.0)
    }

def metas_justica_federal(df_federal: pd.DataFrame) -> dict:
    """Calcula todas as metas para a Justiça Federal."""
    if df_federal.empty: return {}
    return {
        'Meta 1': calcular_metas(df_federal, 'julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025', 100.0),
        'Meta 2A': calcular_metas(df_federal, 'julgm2_a', 'distm2_a', None, 'suspm2_a', (1000 / 8.5)),
        'Meta 2B': calcular_metas(df_federal, 'julgm2_b', 'distm2_b', None, 'suspm2_b', 100.0),
        'Meta 2ANT': calcular_metas(df_federal, 'julgm2_ant', 'distm2_ant', None, 'suspm2_ant', 100.0),
        'Meta 4A': calcular_metas(df_federal, 'julgm4_a', 'distm4_a', None, 'suspm4_a', (1000 / 7.0)),
        'Meta 4B': calcular_metas(df_federal, 'julgm4_b', 'distm4_b', None, 'suspm4_b', 100.0),
        'Meta 6': calcular_metas(df_federal, 'julgm6', 'distm6', None, 'suspm6', (1000 / 3.5)),
        'Meta 7A': calcular_metas(df_federal, 'julgm7_a', 'distm7_a', None, 'suspm7_a', (1000 / 3.5)),
        'Meta 7B': calcular_metas(df_federal, 'julgm7_b', 'distm7_b', None, 'suspm7_b', (1000 / 3.5)),
        'Meta 8A': calcular_metas(df_federal, 'julgm8_a', 'distm8_a', None, 'suspm8_a', (1000 / 7.5)),
        'Meta 8B': calcular_metas(df_federal, 'julgm8_b', 'distm8_b', None, 'suspm8_b', (1000 / 9.0)),
        'Meta 10': calcular_metas(df_federal, 'julgm10', 'distm10', None, 'suspm10', (1000 / 10.0))
    }

def metas_justica_militar_uniao(df_jmu: pd.DataFrame) -> dict:
    """Calcula todas as metas para a Justiça Militar da União (fora o STM)."""
    if df_jmu.empty: return {}
    return {
        'Meta 1': calcular_metas(df_jmu, 'julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025', 100.0),
        'Meta 2A': calcular_metas(df_jmu, 'julgm2_a', 'distm2_a', None, 'suspm2_a', (1000 / 9.5)),
        'Meta 2B': calcular_metas(df_jmu, 'julgm2_b', 'distm2_b', None, 'suspm2_b', (1000 / 9.9)),
        'Meta 2ANT': calcular_metas(df_jmu, 'julgm2_ant', 'distm2_ant', None, 'suspm2_ant', 100.0),
        'Meta 4A': calcular_metas(df_jmu, 'julgm4_a', 'distm4_a', None, 'suspm4_a', (1000 / 9.5)),
        'Meta 4B': calcular_metas(df_jmu, 'julgm4_b', 'distm4_b', None, 'suspm4_b', (1000 / 9.9))
    }

def metas_justica_militar_estadual(df_jme: pd.DataFrame) -> dict:
    """Calcula todas as metas para a Justiça Militar Estadual."""
    if df_jme.empty: return {}
    return {
        'Meta 1': calcular_metas(df_jme, 'julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025', 100.0),
        'Meta 2A': calcular_metas(df_jme, 'julgm2_a', 'distm2_a', None, 'suspm2_a', (1000 / 9.0)),
        'Meta 2B': calcular_metas(df_jme, 'julgm2_b', 'distm2_b', None, 'suspm2_b', (1000 / 9.5)),
        'Meta 2ANT': calcular_metas(df_jme, 'julgm2_ant', 'distm2_ant', None, 'suspm2_ant', 100.0),
        'Meta 4A': calcular_metas(df_jme, 'julgm4_a', 'distm4_a', None, 'suspm4_a', (1000 / 9.5)),
        'Meta 4B': calcular_metas(df_jme, 'julgm4_b', 'distm4_b', None, 'suspm4_b', (1000 / 9.9))
    }

def metas_justica_eleitoral(df_tre: pd.DataFrame) -> dict:
    """Calcula todas as metas para Justiça Eleitoral (TRE's)."""
    if df_tre.empty: return {}
    return {
        'Meta 1': calcular_metas(df_tre, 'julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025', 100.0),
        'Meta 2A': calcular_metas(df_tre, 'julgm2_a', 'distm2_a', None, 'suspm2_a', (1000 / 7.0)),
        'Meta 2B': calcular_metas(df_tre, 'julgm2_b', 'distm2_b', None, 'suspm2_b', (1000 / 9.9)),
        'Meta 2ANT': calcular_metas(df_tre, 'julgm2_ant', 'distm2_ant', None, 'suspm2_ant', 100.0),
        'Meta 4A': calcular_metas(df_tre, 'julgm4_a', 'distm4_a', None, 'suspm4_a', (1000 / 9.0)),
        'Meta 4B': calcular_metas(df_tre, 'julgm4_b', 'distm4_b', None, 'suspm4_b', (1000 / 5.0))
    }

def metas_tst(df_tst: pd.DataFrame) -> dict:
    """Calcula todas as metas para o Tribunal Superior do Trabalho (TST)."""
    if df_tst.empty: return {}
    return {
        'Meta 1': calcular_metas(df_tst, 'julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025', 100.0),
        'Meta 2A': calcular_metas(df_tst, 'julgm2_a', 'distm2_a', None, 'suspm2_a', (1000 / 9.5)),
        'Meta 2B': calcular_metas(df_tst, 'julgm2_b', 'distm2_b', None, 'suspm2_b', (1000 / 9.9)),
        'Meta 2ANT': calcular_metas(df_tst, 'julgm2_ant', 'distm2_ant', None, 'suspm2_ant', 100.0)
    }

def metas_stj(df_stj: pd.DataFrame) -> dict:
    """Calcula todas as metas para o Superior Tribunal de Justiça (STJ)."""
    if df_stj.empty: return {}
    return {
        'Meta 1': calcular_metas(df_stj, 'julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025', 100.0),
        'Meta 2ANT': calcular_metas(df_stj, 'julgm2_ant', 'distm2_ant', None, 'suspm2_ant', 100.0),
        'Meta 4A': calcular_metas(df_stj, 'julgm4_a', 'distm4_a', None, 'suspm4_a', (1000 / 9.0)),
        'Meta 4B': calcular_metas(df_stj, 'julgm4_b', 'distm4_b', None, 'suspm4_b', 100.0),
        'Meta 6': calcular_metas(df_stj, 'julgm6_a', 'distm6_a', None, 'suspm6_a', (1000 / 7.5)),
        'Meta 7A': calcular_metas(df_stj, 'julgm7_a', 'distm7_a', None, 'suspm7_a', (1000 / 7.5)),
        'Meta 7B': calcular_metas(df_stj, 'julgm7_b', 'distm7_b', None, 'suspm7_b', (1000 / 7.5)),
        'Meta 8': calcular_metas(df_stj, 'julgm8', 'distm8', None, 'suspm8', (1000 / 10.0)),
        'Meta 10': calcular_metas(df_stj, 'julgm10', 'distm10', None, 'suspm10', (1000 / 10.0))
    }

FUNCOES_POR_RAMO = {
    'Justiça Estadual': metas_justica_estadual,
    'Justiça do Trabalho': metas_justica_trabalho,
    'Justiça Federal': metas_justica_federal,
    'Justiça Militar da União': metas_justica_militar_uniao,
    'Justiça Militar Estadual': metas_justica_militar_estadual,
    'Justiça Eleitoral': metas_justica_eleitoral
}