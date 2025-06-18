import pandas as pd
import os
import time

from meta_calculadora import calcular_metas, metas_justica_estadual, metas_justica_trabalho, \
                             metas_justica_federal, metas_justica_militar_uniao, \
                             metas_justica_militar_estadual, metas_justica_eleitoral, \
                             metas_tst, metas_stj, FUNCOES_POR_RAMO, META_IDS

from utils import gerar_consolidado, gerar_resumo_metas, gerar_grafico

DIRETORIO_DADOS = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Dados')

def gerar_dados_np() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Gera o DataFrame consolidado e o DataFrame de resumo de metas
    processando arquivos CSV de forma não paralela.
    """
    lista_dataframes = []
    linhas_metas = []

    print("\n--- Iniciando processamento de dados (Versão Não Paralela) ---")
    tc_leitura_calculo = time.time()

    for nome_arquivo in os.listdir(DIRETORIO_DADOS):
        if not nome_arquivo.endswith('.csv'):
            continue

        caminho_arquivo = os.path.join(DIRETORIO_DADOS, nome_arquivo)
        df_lido = None
        try:
            df_lido = pd.read_csv(caminho_arquivo)
            lista_dataframes.append(df_lido)
            print(f"Processando arquivo: {nome_arquivo}")
        except Exception as e:
            print(f"Erro ao ler o arquivo {nome_arquivo}: {e}")
            continue

        for ramo_justica_atual in df_lido['ramo_justica'].unique():
            if ramo_justica_atual == 'Tribunais Superiores':
                df_superiores = df_lido[df_lido['ramo_justica'] == 'Tribunais Superiores'].copy()
                for sigla_tribunal_atual in df_superiores['sigla_tribunal'].unique():
                    df_filtrado_tribunal = df_superiores[df_superiores['sigla_tribunal'] == sigla_tribunal_atual].copy()
                    
                    funcao_a_chamar = None
                    ramo_para_registro_resumo = None

                    if sigla_tribunal_atual == 'STJ':
                        funcao_a_chamar = metas_stj
                        ramo_para_registro_resumo = 'Superior Tribunal de Justiça'
                    elif sigla_tribunal_atual == 'TST':
                        funcao_a_chamar = metas_tst
                        ramo_para_registro_resumo = 'Tribunal Superior do Trabalho'
                    else:
                        continue

                    if not df_filtrado_tribunal.empty and funcao_a_chamar:
                        resultados_calculados = funcao_a_chamar(df_filtrado_tribunal)
                        info_tribunal = {
                            'sigla_tribunal': sigla_tribunal_atual,
                            'ramo_justica': ramo_para_registro_resumo
                        }
                        linha_completa = {**info_tribunal, **resultados_calculados}

                        for meta_id in META_IDS:
                            if f'Meta {meta_id}' not in linha_completa:
                                linha_completa[f'Meta {meta_id}'] = 'NA'
                        linhas_metas.append(linha_completa)

            elif ramo_justica_atual in FUNCOES_POR_RAMO:
                df_ramo_filtrado = df_lido[df_lido['ramo_justica'] == ramo_justica_atual].copy()

                if not df_ramo_filtrado.empty:
                    resultados_calculados = FUNCOES_POR_RAMO[ramo_justica_atual](df_ramo_filtrado)
                    
                    sigla = None
                    if 'sigla_tribunal' in df_ramo_filtrado.columns and not df_ramo_filtrado['sigla_tribunal'].empty:
                        sigla = df_ramo_filtrado['sigla_tribunal'].iloc[0]

                    info_tribunal = {
                        'sigla_tribunal': sigla,
                        'ramo_justica': ramo_justica_atual
                    }
                    linha_completa = {**info_tribunal, **resultados_calculados}

                    for meta_id in META_IDS:
                        if f'Meta {meta_id}' not in linha_completa:
                            linha_completa[f'Meta {meta_id}'] = 'NA'
                    linhas_metas.append(linha_completa)
    
    tc_fim_leitura_calculo = time.time()

    t_consolidacao_df = time.time()
    df_consolidado = pd.concat(lista_dataframes, ignore_index=True)
    
    resumo_metas = pd.DataFrame(linhas_metas)
    
    colunas_resumo = ['sigla_tribunal', 'ramo_justica'] + [f'Meta {m}' for m in META_IDS]
    resumo_metas = resumo_metas.reindex(columns=colunas_resumo).fillna('NA')
    t_fim_consolidacao_df = time.time()

    print(f"\nTempo lendo arquivos e calculando metas (não paralelo): {tc_fim_leitura_calculo - tc_leitura_calculo:.5f} segundos")
    print(f"Tempo consolidando DataFrame e criando resumo: {t_fim_consolidacao_df - t_consolidacao_df:.5f} segundos")

    return df_consolidado, resumo_metas

if __name__ == "__main__":
    t_inicio_total = time.time()
    
    consolidado_df, resumo_metas_df = gerar_dados_np()
    
    gerar_consolidado(consolidado_df, 'Consolidado_NP.csv')
    gerar_resumo_metas(resumo_metas_df, 'ResumoMetas_NP.csv')
    
    tempo_graficos = gerar_grafico(resumo_metas_df)
    
    t_fim_total = time.time()
    print(f"Tempo total de execução (Versão Não Paralela): {(t_fim_total - t_inicio_total):.5f} segundos")