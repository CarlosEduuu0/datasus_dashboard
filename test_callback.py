import os
import pandas as pd
from datetime import datetime
import plotly.graph_objects as go

data_path = "datasus_limpo.parquet"

def safe_load_parquet(path):
    df = pd.read_parquet(path)
    return df

def preprocess_data(df):
    if 'dataNotificacao' in df.columns:
        df['dataNotificacao'] = pd.to_datetime(df['dataNotificacao'], errors='coerce')
    
    if 'dataNotificacao' in df.columns:
        df['ano_mes'] = df['dataNotificacao'].dt.to_period('M').astype(str)
    
    if 'classificacaoFinal' in df.columns:
        confirmados = ['CONFIRMADO LABORATORIAL', 'CONFIRMADO POR CRITÉRIO CLÍNICO', 
                       'CONFIRMADO CLÍNICO-EPIDEMIOLÓGICO', 'CONFIRMADO CLÍNICO-IMAGEM']
        df['positivo'] = df['classificacaoFinal'].isin(confirmados).astype(int)
    
    return df

df = safe_load_parquet(data_path)
df = preprocess_data(df)

def filter_dataframe(municipios, start_date, end_date):
    dff = df.copy()
    if municipios and len(municipios) > 0:
        dff = dff[dff['municipio'].isin(municipios)]
    if start_date and 'dataNotificacao' in dff.columns:
        dff = dff[dff['dataNotificacao'] >= pd.to_datetime(start_date)]
    if end_date and 'dataNotificacao' in dff.columns:
        dff = dff[dff['dataNotificacao'] <= pd.to_datetime(end_date)]
    return dff

print("Testando callback update_serie...")

# Teste 1: Sem filtro de município, período 2022
start = pd.to_datetime('2022-01-01')
end = pd.to_datetime('2022-12-31')
dff = filter_dataframe([], start, end)
print(f"\n✓ Filtrados {len(dff)} registros para 2022")

if 'positivo' in dff.columns:
    agg = dff.groupby('ano_mes').agg({'positivo': 'sum'}).reset_index()
    agg.columns = ['ano_mes', 'confirmados']
    
    total_por_mes = dff.groupby('ano_mes').size().reset_index(name='total')
    agg = agg.merge(total_por_mes, on='ano_mes', how='left')
    agg['negativos'] = agg['total'] - agg['confirmados']
    agg = agg.sort_values('ano_mes')
    
    print(f"✓ Agregação temporal com {len(agg)} meses")
    print(agg)
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=agg['ano_mes'], 
        y=agg['confirmados'], 
        mode='lines+markers', 
        name='confirmados', 
        line=dict(color='#e74c3c', width=3),
        fill='tozeroy',
        fillcolor='rgba(231, 76, 60, 0.2)'
    ))
    fig.add_trace(go.Scatter(
        x=agg['ano_mes'], 
        y=agg['negativos'], 
        mode='lines+markers', 
        name='descartados/suspeitos', 
        line=dict(color='#95a5a6', width=2)
    ))
    
    fig.update_layout(
        title='evolucao temporal de casos',
        xaxis_title='periodo',
        yaxis_title='numero de casos',
        hovermode='x unified',
        plot_bgcolor='#f8f9fa',
        template='plotly_white',
        height=400
    )
    
    print("\n✓ Figura gerada com sucesso")
    print(f"  - X-axis: {len(fig.data[0].x)} meses")
    print(f"  - Y-axis confirmados: {list(fig.data[0].y)}")
    print(f"  - Y-axis negativos: {list(fig.data[1].y)}")
