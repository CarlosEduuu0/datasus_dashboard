import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

import dash
from dash import dcc, html, Input, Output, State, callback_context as ctx
import plotly.express as px
import plotly.graph_objects as go

data_path = "datasus_limpo.parquet"

def safe_load_parquet(path):
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        raise RuntimeError(
            f"falha ao carregar parquet: {e}. certifique-se de ter 'pyarrow' ou 'fastparquet' instalados (pip install pyarrow)."
        )
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

print("carregando dados em:", data_path)
if not os.path.exists(data_path):
    print("arquivo nao encontrado localmente. coloque o arquivo no mesmo diretorio ou ajuste data_path.")
    df = pd.DataFrame()
else:
    df = safe_load_parquet(data_path)
    print(f"dataset carregado: {len(df)} linhas")
    print(f"colunas disponiveis: {list(df.columns)}")
    df = preprocess_data(df)

app = dash.Dash(__name__, suppress_callback_exceptions=True)
server = app.server

data_inicio_padrao = datetime(2022, 1, 1)
data_fim_padrao = datetime(2022, 12, 31)

TEMA_CLARO = {
    'bg_principal': '#ffffff',
    'bg_secundario': '#f8f9fa',
    'texto_principal': '#2c3e50',
    'texto_secundario': '#7f8c8d',
    'border': '#ecf0f1',
    'card_bg': '#ffffff',
    'input_bg': '#ffffff'
}

TEMA_ESCURO = {
    'bg_principal': '#1a1a1a',
    'bg_secundario': '#2d2d2d',
    'texto_principal': '#ecf0f1',
    'texto_secundario': '#bdc3c7',
    'border': '#404040',
    'card_bg': '#262626',
    'input_bg': '#333333'
}

app.layout = html.Div([
    html.Div([
        html.H1("dashboard do datasus", style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': '10px'}),
        html.P("analise epidemiologica de dados do datasus", style={'textAlign': 'center', 'color': '#7f8c8d', 'marginBottom': '30px'})
    ]),
    
    html.Div([
        html.Div([
            html.Label("filtrar por municipio:", style={'fontWeight': 'bold'}),
            dcc.Dropdown(id='municipio-select', options=[], multi=True, placeholder="selecione municipios")
        ], style={'width':'45%','display':'inline-block', 'marginRight': '2%'}),
        
        html.Div([
            html.Label("periodo:", style={'fontWeight': 'bold'}),
            dcc.DatePickerRange(
                id='date-range', 
                start_date=data_inicio_padrao,
                end_date=data_fim_padrao,
                display_format='DD/MM/YYYY'
            )
        ], style={'width':'45%','display':'inline-block'}),
    ], style={'marginBottom': '30px', 'padding': '20px', 'backgroundColor': '#f8f9fa', 'borderRadius': '5px'}),

    html.Div([
        html.Div([
            html.Div(id='total-notificacoes', style={'textAlign': 'center', 'padding': '20px', 'backgroundColor': '#3498db', 'color': 'white', 'borderRadius': '5px'}),
        ], style={'width': '32%', 'display': 'inline-block', 'marginRight': '2%'}),
        
        html.Div([
            html.Div(id='total-confirmados', style={'textAlign': 'center', 'padding': '20px', 'backgroundColor': '#e74c3c', 'color': 'white', 'borderRadius': '5px'}),
        ], style={'width': '32%', 'display': 'inline-block', 'marginRight': '2%'}),
        
        html.Div([
            html.Div(id='municipios-afetados', style={'textAlign': 'center', 'padding': '20px', 'backgroundColor': '#16a085', 'color': 'white', 'borderRadius': '5px'}),
        ], style={'width': '32%', 'display': 'inline-block'}),
    ], style={'marginBottom': '30px'}),

    html.Div([
        html.H3("evolucao temporal", style={'color': '#2c3e50', 'textTransform': 'lowercase'}),
        dcc.Graph(id='serie-temporal'),
    ], style={'marginBottom': '30px'}),

    html.Div([
        html.H3("distribuicoes demograficas", style={'color': '#2c3e50', 'textTransform': 'lowercase'}),
        html.Div([
            html.Div([dcc.Graph(id='dist-sex')], style={'width': '49%', 'display': 'inline-block', 'verticalAlign': 'top'}),
            html.Div([dcc.Graph(id='dist-idade')], style={'width': '49%', 'display': 'inline-block', 'verticalAlign': 'top', 'marginLeft': '2%'}),
        ]),
        html.Div([
            html.Div([dcc.Graph(id='casos-sex')], style={'width': '49%', 'display': 'inline-block', 'verticalAlign': 'top'}),
            html.Div([dcc.Graph(id='casos-idade')], style={'width': '49%', 'display': 'inline-block', 'verticalAlign': 'top', 'marginLeft': '2%'}),
        ]),
    ], style={'marginBottom': '30px'}),

    html.Div(id='secao-sintomas', style={'marginBottom': '30px'}),

    html.Div([
        html.H3("distribuicao geografica", style={'color': '#2c3e50', 'textTransform': 'lowercase'}),
        dcc.Graph(id='map-notificacoes'),
    ], style={'marginBottom': '30px'}),
], style={'padding': '20px', 'fontFamily': 'Arial, sans-serif'})

@app.callback(
    Output('municipio-select','options'), 
    Input('municipio-select','id')
)
def populate_municipios(_):
    if df.empty or 'municipio' not in df.columns:
        return []
    opts = [{'label': str(m), 'value': str(m)} for m in sorted(df['municipio'].dropna().unique())]
    return opts

def filter_dataframe(municipios, start_date, end_date):
    dff = df.copy()
    if municipios and len(municipios) > 0:
        dff = dff[dff['municipio'].isin(municipios)]
    if start_date and 'dataNotificacao' in dff.columns:
        dff = dff[dff['dataNotificacao'] >= pd.to_datetime(start_date)]
    if end_date and 'dataNotificacao' in dff.columns:
        dff = dff[dff['dataNotificacao'] <= pd.to_datetime(end_date)]
    return dff

@app.callback(
    [Output('total-notificacoes','children'),
     Output('total-confirmados','children'),
     Output('municipios-afetados','children')],
    [Input('municipio-select','value'), 
     Input('date-range','start_date'), 
     Input('date-range','end_date')]
)
def update_metrics(municipios, start_date, end_date):
    try:
        dff = filter_dataframe(municipios, start_date, end_date)
        
        total = len(dff)
        confirmados = int(dff['positivo'].sum()) if 'positivo' in dff.columns else 0
        munic = dff['municipio'].nunique() if 'municipio' in dff.columns else 0
        
        return (
            html.Div([html.H2(f"{total:,}"), html.P("total de notificacoes")]),
            html.Div([html.H2(f"{confirmados:,}"), html.P("casos confirmados")]),
            html.Div([html.H2(f"{munic}"), html.P("municipios afetados")])
        )
    except Exception as e:
        print(f"erro em update_metrics: {e}")
        return (
            html.Div([html.H2("erro"), html.P("erro ao calcular")]),
            html.Div([html.H2("erro"), html.P("erro ao calcular")]),
            html.Div([html.H2("erro"), html.P("erro ao calcular")])
        )

@app.callback(
    Output('serie-temporal','figure'),
    [Input('municipio-select','value'), 
     Input('date-range','start_date'), 
     Input('date-range','end_date')]
)
def update_serie(municipios, start_date, end_date):
    try:
        if df.empty:
            fig = go.Figure()
            fig.add_annotation(text="nenhum dado disponivel", showarrow=False, font=dict(size=20))
            return fig
        
        dff = filter_dataframe(municipios, start_date, end_date)
        
        if len(dff) == 0:
            fig = go.Figure()
            fig.add_annotation(text="nenhum dado para os filtros selecionados", showarrow=False, font=dict(size=16))
            return fig
        
        if 'ano_mes' not in dff.columns:
            fig = go.Figure()
            fig.add_annotation(text="coluna de data nao encontrada", showarrow=False, font=dict(size=20))
            return fig

        if 'positivo' in dff.columns:
            agg = dff.groupby('ano_mes').agg({
                'positivo': 'sum'
            }).reset_index()
            agg.columns = ['ano_mes', 'confirmados']
            
            total_por_mes = dff.groupby('ano_mes').size().reset_index(name='total')
            agg = agg.merge(total_por_mes, on='ano_mes', how='left')
            agg['negativos'] = agg['total'] - agg['confirmados']
            agg = agg.sort_values('ano_mes')
            
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
                height=400,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
        else:
            agg = dff.groupby('ano_mes').size().reset_index(name='count')
            agg = agg.sort_values('ano_mes')
            fig = px.line(agg, x='ano_mes', y='count', title='evolucao temporal de notificacoes', markers=True)
            fig.update_traces(line=dict(color='#3498db', width=3))
            fig.update_layout(
                plot_bgcolor='#f8f9fa',
                template='plotly_white',
                height=400,
                xaxis_title='periodo',
                yaxis_title='numero de notificacoes'
            )
        
        return fig
    except Exception as e:
        print(f"erro em update_serie: {e}")
        import traceback
        traceback.print_exc()
        fig = go.Figure()
        fig.add_annotation(text=f"erro ao gerar grafico: {str(e)}", showarrow=False, font=dict(size=14))
        return fig

@app.callback(
    Output('dist-sex','figure'),
    [Input('municipio-select','value'), 
     Input('date-range','start_date'), 
     Input('date-range','end_date')]
)
def dist_sex(municipios, start_date, end_date):
    try:
        if df.empty or 'sexo' not in df.columns:
            fig = go.Figure()
            fig.add_annotation(text="dados de sexo indisponiveis", showarrow=False, font=dict(size=16))
            return fig
        
        dff = filter_dataframe(municipios, start_date, end_date)
        
        if len(dff) == 0:
            fig = go.Figure()
            fig.add_annotation(text="nenhum dado para os filtros selecionados", showarrow=False, font=dict(size=14))
            return fig
        
        counts = dff['sexo'].value_counts().reset_index()
        counts.columns = ['sexo','count']
        
        fig = px.pie(counts, names='sexo', values='count', title='distribuicao por sexo',
                     color_discrete_sequence=px.colors.qualitative.Set2,
                     hole=0.3)
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(title_x=0.5)
        return fig
    except Exception as e:
        print(f"erro em dist_sex: {e}")
        fig = go.Figure()
        fig.add_annotation(text=f"erro: {str(e)}", showarrow=False, font=dict(size=14))
        return fig

@app.callback(
    Output('dist-idade','figure'),
    [Input('municipio-select','value'), 
     Input('date-range','start_date'), 
     Input('date-range','end_date')]
)
def dist_idade(municipios, start_date, end_date):
    try:
        if df.empty or 'idade' not in df.columns:
            fig = go.Figure()
            fig.add_annotation(text="dados de idade indisponiveis", showarrow=False, font=dict(size=16))
            return fig
        
        dff = filter_dataframe(municipios, start_date, end_date)
        
        if len(dff) == 0:
            fig = go.Figure()
            fig.add_annotation(text="nenhum dado para os filtros selecionados", showarrow=False, font=dict(size=14))
            return fig
        
        fig = px.histogram(dff, x='idade', nbins=40, title='distribuicao de idades',
                          color_discrete_sequence=['#3498db'])
        fig.update_layout(
            plot_bgcolor='#f8f9fa',
            xaxis_title='idade',
            yaxis_title='frequencia',
            bargap=0.1,
            title_x=0.5
        )
        return fig
    except Exception as e:
        print(f"erro em dist_idade: {e}")
        fig = go.Figure()
        fig.add_annotation(text=f"erro: {str(e)}", showarrow=False, font=dict(size=14))
        return fig

@app.callback(
    Output('casos-sex','figure'),
    [Input('municipio-select','value'), 
     Input('date-range','start_date'), 
     Input('date-range','end_date')]
)
def casos_sex(municipios, start_date, end_date):
    try:
        if df.empty or 'sexo' not in df.columns:
            fig = go.Figure()
            fig.add_annotation(text="dados de sexo indisponiveis", showarrow=False, font=dict(size=16))
            return fig
        
        dff = filter_dataframe(municipios, start_date, end_date)
        
        if len(dff) == 0:
            fig = go.Figure()
            fig.add_annotation(text="nenhum dado para os filtros selecionados", showarrow=False, font=dict(size=14))
            return fig
        
        counts = dff['sexo'].value_counts().reset_index()
        counts.columns = ['sexo', 'casos']
        counts = counts.sort_values('casos', ascending=True)
        
        fig = px.bar(counts, x='casos', y='sexo', orientation='h',
                     title='numero de casos por sexo',
                     color='casos',
                     color_continuous_scale='RdYlBu')
        fig.update_layout(
            plot_bgcolor='#f8f9fa',
            xaxis_title='numero de casos',
            yaxis_title='sexo',
            title_x=0.5,
            height=300
        )
        return fig
    except Exception as e:
        print(f"erro em casos_sex: {e}")
        fig = go.Figure()
        fig.add_annotation(text=f"erro: {str(e)}", showarrow=False, font=dict(size=14))
        return fig

@app.callback(
    Output('casos-idade','figure'),
    [Input('municipio-select','value'), 
     Input('date-range','start_date'), 
     Input('date-range','end_date')]
)
def casos_idade(municipios, start_date, end_date):
    try:
        if df.empty or 'idade' not in df.columns:
            fig = go.Figure()
            fig.add_annotation(text="dados de idade indisponiveis", showarrow=False, font=dict(size=16))
            return fig
        
        dff = filter_dataframe(municipios, start_date, end_date)
        
        if len(dff) == 0:
            fig = go.Figure()
            fig.add_annotation(text="nenhum dado para os filtros selecionados", showarrow=False, font=dict(size=14))
            return fig
        
        dff_limpo = dff.dropna(subset=['idade'])
        dff_limpo = dff_limpo[dff_limpo['idade'] >= 0]
        
        if len(dff_limpo) == 0:
            fig = go.Figure()
            fig.add_annotation(text="nenhum dado de idade valido", showarrow=False, font=dict(size=14))
            return fig
        
        counts = dff_limpo.groupby(pd.cut(dff_limpo['idade'], bins=10), observed=True).size().reset_index(name='casos')
        counts['faixa_etaria'] = counts['idade'].apply(lambda x: f"{int(x.left)}-{int(x.right)}")
        
        fig = px.bar(counts, x='faixa_etaria', y='casos',
                     title='numero de casos por faixa etaria',
                     color='casos',
                     color_continuous_scale='Reds')
        fig.update_layout(
            plot_bgcolor='#f8f9fa',
            xaxis_title='faixa etaria',
            yaxis_title='numero de casos',
            title_x=0.5,
            height=300,
            xaxis_tickangle=-45
        )
        return fig
    except Exception as e:
        print(f"erro em casos_idade: {e}")
        fig = go.Figure()
        fig.add_annotation(text=f"erro: {str(e)}", showarrow=False, font=dict(size=14))
        return fig

@app.callback(
    Output('secao-sintomas','children'),
    [Input('municipio-select','value'), 
     Input('date-range','start_date'), 
     Input('date-range','end_date')]
)
def secao_sintomas(municipios, start_date, end_date):
    try:
        if df.empty or 'sintomas' not in df.columns:
            return html.Div()
        
        dff = filter_dataframe(municipios, start_date, end_date)
        
        if len(dff) == 0:
            return html.Div()
        
        # Fazer split dos sintomas multivalorados (separados por vírgula)
        sintomas_individuais = []
        for sintomas_str in dff['sintomas'].dropna():
            sintomas_str = str(sintomas_str).strip()
            if sintomas_str and sintomas_str.upper() != 'NÃO INFORMADO':
                # Split por vírgula e adicionar cada sintoma individual
                sintomas_lista = [s.strip() for s in sintomas_str.split(',') if s.strip()]
                sintomas_individuais.extend(sintomas_lista)
        
        if not sintomas_individuais:
            return html.Div()
        
        # Contar frequência de cada sintoma individual
        sintomas_series = pd.Series(sintomas_individuais)
        sintomas_counts = sintomas_series.value_counts().nlargest(15).reset_index()
        sintomas_counts.columns = ['sintomas','count']
        
        fig = px.bar(sintomas_counts, y='sintomas', x='count', orientation='h',
                    title='top 15 sintomas mais frequentes',
                    color_discrete_sequence=['#9b59b6'])
        fig.update_layout(
            plot_bgcolor='#f8f9fa',
            yaxis={'categoryorder':'total ascending'},
            title_x=0.5,
            height=500
        )
        
        return html.Div([
            html.H3("analise de sintomas", style={'color': '#2c3e50', 'textTransform': 'lowercase'}),
            dcc.Graph(figure=fig)
        ])
    except Exception as e:
        print(f"erro em secao_sintomas: {e}")
        return html.Div()

@app.callback(
    Output('map-notificacoes','figure'),
    [Input('municipio-select','value'), 
     Input('date-range','start_date'), 
     Input('date-range','end_date')]
)
def map_notificacoes(municipios, start_date, end_date):
    try:
        if df.empty:
            fig = go.Figure()
            fig.add_annotation(text="nenhum dado disponivel", showarrow=False, font=dict(size=20))
            return fig
        
        dff = filter_dataframe(municipios, start_date, end_date)
        
        if len(dff) == 0:
            fig = go.Figure()
            fig.add_annotation(text="nenhum dado para os filtros selecionados", showarrow=False, font=dict(size=16))
            return fig
        
        if 'longitude' in dff.columns and 'latitude' in dff.columns:
            dff_map = dff.dropna(subset=['longitude','latitude'])
            if len(dff_map) > 0:
                fig = px.density_mapbox(dff_map, lat='latitude', lon='longitude',
                                       radius=10,
                                       center=dict(lat=-1.4558, lon=-48.5044),
                                       zoom=5,
                                       mapbox_style='open-street-map',
                                       title='mapa de densidade de notificacoes',
                                       color_continuous_scale='reds')
                fig.update_layout(title_x=0.5)
                return fig
        
        if 'municipio' in dff.columns:
            agg = dff.groupby('municipio').size().reset_index(name='count').nlargest(30, 'count')
            fig = px.bar(agg, y='municipio', x='count', orientation='h',
                        title='top 30 municipios por notificacoes',
                        color='count',
                        color_continuous_scale='reds')
            fig.update_layout(
                plot_bgcolor='#f8f9fa',
                yaxis={'categoryorder':'total ascending'},
                title_x=0.5,
                height=700
            )
            return fig
        
        fig = go.Figure()
        fig.add_annotation(text="dados geograficos indisponiveis", showarrow=False, font=dict(size=20))
        return fig
    except Exception as e:
        print(f"erro em map_notificacoes: {e}")
        fig = go.Figure()
        fig.add_annotation(text=f"erro: {str(e)}", showarrow=False, font=dict(size=14))
        return fig

if __name__ == '__main__':
    app.run(debug=True)