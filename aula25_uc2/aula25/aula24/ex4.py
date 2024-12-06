import pandas as pd
import polars as pl
from datetime import datetime
import os
import gc  # Garbage Collector



ENDERECO_DADOS = r'./dados/'

try:
    print('Obtendo dados')

    inicio = datetime.now()

    lista_arquivos = []
    
    # Lista final dos arquivos de dados que vieram do diretório
    lista_dir_arquivos = os.listdir(ENDERECO_DADOS)

    # Pegando os arquivos CSVs do diretório
    for arquivo in lista_dir_arquivos:
        if arquivo.endswith('.csv'):
            lista_arquivos.append(arquivo)

    print(lista_arquivos)

    # Leitura dos arquivos
    for arquivo in lista_arquivos:
        print(f'Processando arquivo {arquivo}')

        # Leitura de cada um dos dataframes
        df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding='iso-8859-1')

        # Concatenação dos Dataframes
        # Verifica se o DataFrame df_bolsa_familia já existe,
        # Se existir, acontece a concatenação, 
        # Senão, (o Loop estpa na primeira execução), então
        # o df_dados é atribuído a df_bolsa_familia
        if 'df_bolsa_familia' in locals():
            df_bolsa_familia = pl.concat([df_bolsa_familia, df])
        else:
            df_bolsa_familia = df
        
        # Remover df_dados após o uso para liberar memória
       
        # Prints
        print(df_bolsa_familia.head())
        # print(df_bolsa_familia.shape)
        # print(df_bolsa_familia.columns)
        # print(df_bolsa_familia.dtypes)

        print(f'Arquivo {arquivo} processados com sucesso!')

    # ######  Fim do For

    # Converte a coluna 'VALOR PARCELA' para o tipo float
    df_bolsa_familia = df_bolsa_familia.with_columns(
        pl.col('VALOR PARCELA').str.replace(',', '.').cast(pl.Float64)
    )

    print('\nDados dos DataFrames concatenados com sucesso!')
    print('Incinando a gravação do arquivo Parquet...')

    # Criar arquivo Parquet
    df_bolsa_familia.write_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')    
    # Deletar df_bolsa_familia da memória
    del df_bolsa_familia
    # Coletar resíduos da memória
    # gc.collect()

    fim = datetime.now()
    
    print(f'Tempo de execução: {fim - inicio}')
    print('Gravação do arquivo Parquet realizada com sucesso!')

except ImportError as e:
    print(f'Erro ao processar os dataframes: {e}')