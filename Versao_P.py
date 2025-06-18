import pandas as pd
import os
import time
import concurrent.futures

from meta_calculadora import calcular_metas, metas_justica_estadual, metas_justica_trabalho, \
                             metas_justica_federal, metas_justica_militar_uniao, \
                             metas_justica_militar_estadual, metas_justica_eleitoral, \
                             metas_tst, metas_stj, FUNCOES_POR_RAMO, META_IDS

from utils import gerar_consolidado, gerar_resumo_metas, gerar_grafico

DIRETORIO_DADOS = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Dados')

def processar_arquivo_csv(caminho_arquivo: str) -> tuple[pd.DataFrame | None, list]:
    """
    Lê um arquivo CSV, calcula as metas para os ramos de justiça contidos nele,
    e retorna o DataFrame lido e uma lista de linhas de metas.
    """
    linhas_metas_arquivo = []
    df_lido = None
    try:
        df_lido = pd.read_csv(caminho_arquivo)
    except Exception as e:
        print(f"Erro ao ler o arquivo {os.path.basename(caminho_arquivo)}: {e}")
        return None, []

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
                    linhas_metas_arquivo.append(linha_completa)

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
                linhas_metas_arquivo.append(linha_completa)
    return df_lido, linhas_metas_arquivo

def gerar_metas_paralelizado() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Gera o DataFrame consolidado e o DataFrame de resumo de metas
    processando arquivos CSV de forma paralela.
    """
    lista_arquivos = [
        os.path.join(DIRETORIO_DADOS, nome_arquivo)
        for nome_arquivo in os.listdir(DIRETORIO_DADOS)
        if nome_arquivo.endswith('.csv')
    ]

    total_dfs = []
    todas_linhas_metas = []

    print("\n--- Iniciando processamento de dados (Versão Paralela) ---")
    tc_leitura_calculo = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        future_to_file = {executor.submit(processar_arquivo_csv, arq_path): arq_path for arq_path in lista_arquivos}
        
        for future in concurrent.futures.as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                df_lido, linhas_do_arquivo = future.result()
                if df_lido is not None:
                    total_dfs.append(df_lido)
                todas_linhas_metas.extend(linhas_do_arquivo)
                print(f"Processamento concluído para: {os.path.basename(file_path)}")
            except Exception as exc:
                print(f"Arquivo {os.path.basename(file_path)} gerou uma exceção: {exc}")

    tc_fim_leitura_calculo = time.time()

    t_consolidacao_df = time.time()
    df_consolidado = pd.concat(total_dfs, ignore_index=True)
    
    resumo_metas = pd.DataFrame(todas_linhas_metas)

    colunas_resumo = ['sigla_tribunal', 'ramo_justica'] + [f'Meta {m}' for m in META_IDS]
    resumo_metas = resumo_metas.reindex(columns=colunas_resumo).fillna('NA')
    t_fim_consolidacao_df = time.time()

    print(f"\nTempo lendo arquivos e calculando metas (paralelo): {tc_fim_leitura_calculo - tc_leitura_calculo:.5f} segundos")
    print(f"Tempo consolidando DataFrame e criando resumo: {t_fim_consolidacao_df - t_consolidacao_df:.5f} segundos")

    return df_consolidado, resumo_metas

if __name__ == "__main__":
    t_inicio_total = time.time()
    
    consolidado_df, resumo_metas_df = gerar_metas_paralelizado()
    
    gerar_consolidado(consolidado_df, 'Consolidado_P.csv')
    gerar_resumo_metas(resumo_metas_df, 'ResumoMetas_P.csv')
    
    tempo_graficos = gerar_grafico(resumo_metas_df)
    
    t_fim_total = time.time()
    print(f"Tempo total de execução (Versão Paralela): {(t_fim_total - t_inicio_total):.5f} segundos")