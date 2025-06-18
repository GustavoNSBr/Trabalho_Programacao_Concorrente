import pandas as pd
import os
import time
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

from meta_calculadora import META_IDS

def gerar_consolidado(df_consolidado: pd.DataFrame, filename: str = 'Consolidado.csv'):
    """Salva o DataFrame consolidado em um arquivo CSV."""
    t0 = time.time()
    try:
        df_consolidado.to_csv(filename, index=False)
        tf = time.time()
        print(f"Tempo criando {filename}: {tf - t0:.5f} segundos")
    except Exception as e:
        print(f"Erro ao salvar {filename}: {e}")

def gerar_resumo_metas(df_resumo_metas: pd.DataFrame, filename: str = 'ResumoMetas.csv'):
    """Salva o DataFrame de resumo de metas em um arquivo CSV."""
    t0 = time.time()
    try:
        df_resumo_metas.to_csv(filename, index=False)
        tf = time.time()
        print(f"Tempo criando {filename}: {tf - t0:.5f} segundos")
    except Exception as e:
        print(f"Erro ao salvar {filename}: {e}")

def gerar_grafico(df_resumo_metas: pd.DataFrame) -> float:
    """Gera heatmaps de desempenho das metas para diferentes grupos de justiça."""
    if df_resumo_metas.empty:
        print("DataFrame de resumo de metas vazio. Nenhum gráfico será gerado.")
        return 0.0

    colunas_metas = [col for col in df_resumo_metas.columns if col.startswith('Meta ')]
    
    grupos_justica = {
        'Justiça Estadual': ['Justiça Estadual'],
        'Justiça Eleitoral': ['Justiça Eleitoral'],
        'Tribunais Superiores': ['Superior Tribunal de Justiça', 'Tribunal Superior do Trabalho']
    }

    todos_ramos_no_df = df_resumo_metas['ramo_justica'].unique()
    ramos_com_grafico_proprio = [ramo for sublist in grupos_justica.values() for ramo in sublist]
    grupos_justica['Demais Ramos'] = [ramo for ramo in todos_ramos_no_df if ramo not in ramos_com_grafico_proprio]

    tempo_total_graficos = 0
    pd.set_option('future.no_silent_downcasting', True)

    for nome_grupo, ramos_a_filtrar in grupos_justica.items():
        if not ramos_a_filtrar:
            continue

        tg = time.time()
        df_grupo = df_resumo_metas[df_resumo_metas['ramo_justica'].isin(ramos_a_filtrar)].copy()

        df_para_heatmap_grupo = df_grupo.set_index('sigla_tribunal')[colunas_metas].copy()
        
        df_para_heatmap_grupo = df_para_heatmap_grupo.replace('NA', np.nan)
        df_para_heatmap_grupo = df_para_heatmap_grupo.astype(float)
        
        df_para_heatmap_grupo = df_para_heatmap_grupo.dropna(axis=1, how='all')

        if df_para_heatmap_grupo.empty:
            continue

        plt.figure(figsize=(15, max(8, len(df_para_heatmap_grupo) * 0.8)))
        
        sns.heatmap(df_para_heatmap_grupo,
                    annot=True,
                    fmt=".1f",
                    cmap="viridis",
                    linewidths=.5,
                    linecolor='black',
                    cbar_kws={'label': 'Valor da Meta'},
                    annot_kws={"size": 10})

        plt.title(f'Desempenho das Metas por Tribunal ({nome_grupo})', fontsize=16)
        plt.ylabel('Sigla do Tribunal', fontsize=12)
        plt.xlabel('Metas', fontsize=12)
        plt.xticks(rotation=45, ha='right', fontsize=10)
        plt.yticks(rotation=0, fontsize=10)
        plt.tight_layout()
        plt.show()

        tf_grafico = time.time()
        tempo_total_graficos += (tf_grafico - tg)

    pd.set_option('future.no_silent_downcasting', False)

    return tempo_total_graficos