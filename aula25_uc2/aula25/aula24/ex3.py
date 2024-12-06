# import pandas as pd 
import polars as pl 
from datetime import datetime
import os 
import gc
import numpy as np 
from matplotlib import pyplot as plt 



ENDERECO_DADOS = r'./dados/'

try:

    print('Iniciando leitura do arquivo parquet....')

    # ENDERECO_DADOS = r'./dados/'

    inicio = datetime.now()
    # df_bolsa_familia = pl.read_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')
    df_bolsa_familia_plan = pl.scan_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')
    df_bolsa_familia = df_bolsa_familia_plan.collect()

    print(df_bolsa_familia.head())

    fim = datetime.now()

    print(f'Tempo de execução para leitura de parquet: {fim - inicio}')
    print('\nArquivo parquet lido com sucesso!!')


    
    # inicio = datetime.now()

    # lista_arquivos = []

    # lista_dir_arquivos = os.listdir(ENDERECO_DADOS) 

    # for arquivo in lista_dir_arquivos:
    #     if arquivo.endswith('.csv'):
    #         lista_arquivos.append(arquivo)

    # print(lista_arquivos)


    # for arquivo in lista_arquivos:
    #     print(f'Processando arquivo {arquivo}')

    #     df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding='iso-8859-1')

    #     if 'df_bolsa_familia' in locals():
    #         df_bolsa_familia = pl.concat([df_bolsa_familia, df])
    #     else:
    #         df_bolsa_familia =df

    #     # print(df.head())

    #     del df

    #     print(df_bolsa_familia.head())

    #     print(f'Arquivo {arquivo} processados com sucesso!')    
    
    # df_bolsa_familia.write_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')

    # del df_bolsa_familia

    # gc.collect()


    # # ENDERECO_DADOS = r'./dados/'

    # hora_import = datetime.now()
    # # df_janeiro = pl.read_csv(ENDERECO_DADOS + '202401_NovoBolsaFamilia.csv', separator=';' ,encoding='iso-8859-1')
    # # print(df_janeiro.head())

    # hora_impressao = datetime.now()

    # print(f'Tempo de execução: {hora_impressao - hora_import}')

except ImportError as e:
    print('Erro ao obter dados: ')


 




try:
    print('Visualizando a distribuição dos valores das parcelas em um boxplot...')


    hora_inicio = datetime.now()


    array_valor_parcela = np.array(df_bolsa_familia['VALOR PARCELA'])
    
    plt.boxplot(array_valor_parcela, vert=False)
    plt.title('Distribuição dos valores das parcelas')


    hora_fim = datetime.now()

    plt.show()

    print(f'Tempo de execução: {hora_fim - hora_inicio}')
    

except ImportError as e:
    print('Erro ao obter dados: ')
