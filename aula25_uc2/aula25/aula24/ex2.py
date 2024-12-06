import pandas as pd 
import polars as pl 
from datetime import datetime
import gc


try:
    print('Obtendo dados')
    ENDERECO_DADOS = r'./dados/'

    inicio = datetime.now

    lista_arquivos = ['202401_NovoBolsaFamilia.csv', '202402_NovoBolsaFamilia.csv']

    for arquivo in lista_arquivos:
        print(f'Processando arquivo {arquivo}')

        df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding='iso-8859-1')

        if 'df_bolsa_familia' in locals():
            df_bolsa_familia = pl.concat([df_bolsa_familia, df])
        else:
            df_bolsa_familia =df

        print(df.head())

        del df

        gc.collect()


    
 





    # ENDERECO_DADOS = r'./dados/'

    hora_import = datetime.now()
    # df_janeiro = pl.read_csv(ENDERECO_DADOS + '202401_NovoBolsaFamilia.csv', separator=';' ,encoding='iso-8859-1')
    # print(df_janeiro.head())

    hora_impressao = datetime.now()

    print(f'Tempo de execução: {hora_impressao - hora_import}')





except ImportError as e:
    print('Erro ao obter dados: ')