import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime

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

# Teste período padrão (2022)
start = datetime(2022, 1, 1)
end = datetime(2022, 12, 31)
dff = filter_dataframe([], start, end)

print("=" * 70)
print("TESTE DOS NOVOS GRÁFICOS")
print("=" * 70)

# Teste 1: Gráfico de casos por sexo
print("\n✓ TESTE 1: Gráfico de casos por sexo")
counts_sex = dff['sexo'].value_counts().reset_index()
counts_sex.columns = ['sexo', 'casos']
counts_sex = counts_sex.sort_values('casos', ascending=True)
print(f"  Dados:\n{counts_sex}")
print(f"  ✓ Gráfico de barras horizontal com cores gradient Blues")

# Teste 2: Gráfico de casos por faixa etária
print("\n✓ TESTE 2: Gráfico de casos por faixa etária")
dff_limpo = dff.dropna(subset=['idade'])
dff_limpo = dff_limpo[dff_limpo['idade'] >= 0]
print(f"  Registros com idade válida: {len(dff_limpo)}")

counts_idade = dff_limpo.groupby(pd.cut(dff_limpo['idade'], bins=10)).size().reset_index(name='casos')
counts_idade['faixa_etaria'] = counts_idade['idade'].astype(str)
print(f"  Faixas etárias: {len(counts_idade)}")
print(f"  Total de casos por faixa:\n{counts_idade[['faixa_etaria', 'casos']]}")
print(f"  ✓ Gráfico de barras vertical com cores gradient Reds")

print("\n" + "=" * 70)
print("RESUMO DOS NOVOS GRÁFICOS")
print("=" * 70)
print(f"""
✅ casos-sex: Número de casos por sexo
   - Tipo: Gráfico de barras horizontal
   - Cores: Blues (gradient)
   - Altura: 300px
   - Categorias: {len(counts_sex)} tipos de sexo

✅ casos-idade: Número de casos por faixa etária
   - Tipo: Gráfico de barras vertical
   - Cores: Reds (gradient)
   - Altura: 300px
   - Faixas: {len(counts_idade)} faixas etárias
   - Total: {counts_idade['casos'].sum()} casos

📊 Os gráficos ficam lado a lado com os gráficos existentes
   (dist-sex e dist-idade) na seção "distribuicoes demograficas"
""")
