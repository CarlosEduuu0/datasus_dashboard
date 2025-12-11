import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

data_path = "datasus_limpo.parquet"

def safe_load_parquet(path):
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        raise RuntimeError(f"falha ao carregar parquet: {e}")
    return df

def preprocess_data(df):
    if 'dataNotificacao' in df.columns:
        df['dataNotificacao'] = pd.to_datetime(df['dataNotificacao'], errors='coerce')
    
    if 'dataNotificacao' in df.columns:
        df['ano_mes'] = df['dataNotificacao'].dt.to_period('M').astype(str)
        df['ano_semana'] = df['dataNotificacao'].dt.to_period('W').astype(str)
    
    if 'classificacaoFinal' in df.columns:
        confirmados = ['CONFIRMADO LABORATORIAL', 'CONFIRMADO POR CRITÉRIO CLÍNICO', 
                       'CONFIRMADO CLÍNICO-EPIDEMIOLÓGICO', 'CONFIRMADO CLÍNICO-IMAGEM']
        df['positivo'] = df['classificacaoFinal'].isin(confirmados).astype(int)
    
    return df

print("carregando dados...")
if os.path.exists(data_path):
    df = safe_load_parquet(data_path)
    df = preprocess_data(df)
    print(f"✓ Dataset carregado: {len(df)} linhas")
    print(f"✓ Colunas essenciais: ano_mes={('ano_mes' in df.columns)}, positivo={('positivo' in df.columns)}")
    print(f"\nAmostras ano_mes: {df['ano_mes'].unique()[:8]}")
    print(f"\nPrimeira data: {df['dataNotificacao'].min()}")
    print(f"Última data: {df['dataNotificacao'].max()}")
    print(f"\nTotal confirmados: {df['positivo'].sum()}")
    print(f"Taxa de confirmação: {(df['positivo'].sum() / len(df) * 100):.2f}%")
    
    # Testar filtro de 2022
    mask_2022 = (df['dataNotificacao'] >= pd.to_datetime('2022-01-01')) & (df['dataNotificacao'] <= pd.to_datetime('2022-12-31'))
    df_2022 = df[mask_2022]
    print(f"\nDados em 2022: {len(df_2022)} registros")
    
    # Testar agregação temporal
    agg = df_2022.groupby('ano_mes').agg({'positivo': 'sum'}).reset_index()
    agg.columns = ['ano_mes', 'confirmados']
    total_por_mes = df_2022.groupby('ano_mes').size().reset_index(name='total')
    agg = agg.merge(total_por_mes, on='ano_mes', how='left')
    agg['negativos'] = agg['total'] - agg['confirmados']
    agg = agg.sort_values('ano_mes')
    print(f"\n✓ Agregação temporal funcionando:")
    print(agg.head(12))
else:
    print("erro: arquivo nao encontrado")
