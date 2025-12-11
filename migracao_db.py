import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()
DB_CONFIG = {
    'host': os.getenv('PG_HOST'),
    'database': os.getenv('PG_DB'),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD'),
    'port': int(os.getenv('PG_PORT'))
}

# Mapeamentos de valores do CSV para o banco de dados
MAPEAMENTOS = {
    'sexo': {
        'MASCULINO': 'Masculino',
        'FEMININO': 'Feminino'
    },
    'raca': {
        'BRANCA': 'Branca',
        'PRETA': 'Preta',
        'PARDA': 'Parda',
        'AMARELA': 'Amarela',
        'INDÍGENA': 'Indígena',
        'INDIGENA': 'Indígena',
        'IGNORADO': 'Ignorado'
    },
    'evolucao_caso': {
        'CANCELADO': 'Cancelado',
        'IGNORADO': 'Ignorado',
        'EM TRATAMENTO DOMICILIAR': 'Em tratamento domiciliar',
        'INTERNADO EM UTI': 'Internado em UTI',
        'INTERNADO': 'Internado',
        'ÓBITO': 'Óbito',
        'OBITO': 'Óbito',
        'CURA': 'Cura'
    },
    'classificacao_final': {
        'CONFIRMADO LABORATORIAL': 'Confirmado Laboratorial',
        'CONFIRMADO CLÍNICO-EPIDEMIOLÓGICO': 'Confirmado Clínico-Epidemiológico',
        'CONFIRMADO CLINICO-EPIDEMIOLOGICO': 'Confirmado Clínico-Epidemiológico',
        'DESCARTADO': 'Descartado',
        'SÍNDROME GRIPAL NÃO ESPECIFICADA': 'Síndrome Gripal Não Especificada',
        'SINDROME GRIPAL NAO ESPECIFICADA': 'Síndrome Gripal Não Especificada',
        'CONFIRMADO': 'Confirmado',
        'CONFIRMADO POR CRITÉRIO CLÍNICO': 'Confirmado por Critério Clínico',
        'CONFIRMADO POR CRITERIO CLINICO': 'Confirmado por Critério Clínico',
        'CLÍNICO-IMAGEM': 'Clínico-Imagem',
        'CLINICO-IMAGEM': 'Clínico-Imagem',
        'CONFIRMADO CLÍNICO-IMAGEM': 'Clínico-Imagem',
    },
    'sintoma': {
        'ASSINTOMÁTICO': 'Assintomático',
        'ASSINTOMATICO': 'Assintomático',
        'DOR DE CABEÇA': 'Dor de Cabeça',
        'DOR DE CABECA': 'Dor de Cabeça',
        'FEBRE': 'Febre',
        'DISTÚRBIOS GUSTATIVOS': 'Distúrbios Gustativos',
        'DISTURBIOS GUSTATIVOS': 'Distúrbios Gustativos',
        'DISTÚRBIOS OLFATIVOS': 'Distúrbios Olfativos',
        'DISTURBIOS OLFATIVOS': 'Distúrbios Olfativos',
        'DOR DE GARGANTA': 'Dor de Garganta',
        'DISPNEIA': 'Dispneia',
        'DISPNÉIA': 'Dispneia',
        'TOSSE': 'Tosse',
        'CORIZA': 'Coriza',
        'OUTROS': 'Outros'
    },
    'condicao': {
        'DOENÇAS RESPIRATÓRIAS CRÔNICAS DESCOMPENSADAS': 'Doenças respiratórias crônicas descompensadas',
        'DOENCAS RESPIRATORIAS CRONICAS DESCOMPENSADAS': 'Doenças respiratórias crônicas descompensadas',
        'OUTROS': 'Outros',
        'DOENÇAS CARDÍACAS CRÔNICAS': 'Doenças cardíacas crônicas',
        'DOENCAS CARDIACAS CRONICAS': 'Doenças cardíacas crônicas',
        'DIABETES': 'Diabetes',
        'DOENÇAS RENAIS CRÔNICAS EM ESTÁGIO AVANÇADO (GRAUS 3, 4 OU 5)': 'Doenças renais crônicas em estágio avançado (graus 3, 4 ou 5)',
        'DOENCAS RENAIS CRONICAS EM ESTAGIO AVANCADO (GRAUS 3, 4 OU 5)': 'Doenças renais crônicas em estágio avançado (graus 3, 4 ou 5)',
        'IMUNOSSUPRESSÃO': 'Imunossupressão',
        'IMUNOSSUPRESSAO': 'Imunossupressão',
        'PORTADOR DE DOENÇAS CROMOSSÔMICAS OU ESTADO DE FRAGILIDADE IMUNOLÓGICA': 'Portador de doenças cromossômicas ou estado de fragilidade imunológica',
        'PORTADOR DE DOENCAS CROMOSSOMICAS OU ESTADO DE FRAGILIDADE IMUNOLOGICA': 'Portador de doenças cromossômicas ou estado de fragilidade imunológica',
        'PUÉRPERA (ATÉ 45 DIAS DO PARTO)': 'Puérpera (até 45 dias do parto)',
        'PUERPERA (ATE 45 DIAS DO PARTO)': 'Puérpera (até 45 dias do parto)',
        'OBESIDADE': 'Obesidade',
        'GESTANTE': 'Gestante'
    }
}

class MigradorDadosSUS:
    def __init__(self, arquivo_parquet, db_config):
        self.df = pd.read_parquet(arquivo_parquet)
        self.db_config = db_config
        self.conn = None
        self.cursor = None
        
    def conectar(self):
       
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("✓ Conectado ao banco de dados")
        except Exception as e:
            print(f"✗ Erro ao conectar: {e}")
            raise
    
    def desconectar(self):
        
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✓ Conexão fechada")
    
    def mapear_valor(self, categoria, valor):
        """Mapeia valores do CSV para os valores esperados pelo banco"""
        if pd.isna(valor):
            return None
        
        valor_upper = str(valor).strip().upper()
        if categoria in MAPEAMENTOS:
            return MAPEAMENTOS[categoria].get(valor_upper, valor)
        return valor
    
    # ============= FASE 1: TABELAS DE DOMÍNIO =============
    
    def migrar_sexo(self):
        """Migra tabela sexo"""
        print("\n--- Migrando SEXO ---")
        
        valores_unicos = self.df['sexo'].dropna().unique()
        valores_mapeados = [self.mapear_valor('sexo', v) for v in valores_unicos if self.mapear_valor('sexo', v)]
        
        # Remove duplicatas e valores None
        valores_mapeados = list(set([v for v in valores_mapeados if v]))
        
        for valor in valores_mapeados:
            try:
                self.cursor.execute(
                    "INSERT INTO sexo (descricao) VALUES (%s) ON CONFLICT (descricao) DO NOTHING",
                    (valor,)
                )
            except Exception as e:
                print(f"  ✗ Erro ao inserir '{valor}': {e}")
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM sexo")
        print(f"✓ Sexo: {self.cursor.fetchone()[0]} registros inseridos")
    
    def migrar_raca(self):
        """Migra tabela raca"""
        print("\n--- Migrando RAÇA/COR ---")
        
        valores_unicos = self.df['racaCor'].dropna().unique()
        valores_mapeados = [self.mapear_valor('raca', v) for v in valores_unicos if self.mapear_valor('raca', v)]
        valores_mapeados = list(set([v for v in valores_mapeados if v]))
        
        for valor in valores_mapeados:
            try:
                self.cursor.execute(
                    "INSERT INTO raca (descricao) VALUES (%s) ON CONFLICT (descricao) DO NOTHING",
                    (valor,)
                )
            except Exception as e:
                print(f"  ✗ Erro ao inserir '{valor}': {e}")
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM raca")
        print(f"✓ Raça/Cor: {self.cursor.fetchone()[0]} registros inseridos")
    
    def migrar_evolucao_caso(self):
        """Migra tabela evolucao_caso"""
        print("\n--- Migrando EVOLUÇÃO DO CASO ---")
        
        valores_unicos = self.df['evolucaoCaso'].dropna().unique()
        valores_mapeados = [self.mapear_valor('evolucao_caso', v) for v in valores_unicos]
        valores_mapeados = list(set([v for v in valores_mapeados if v]))
        
        for valor in valores_mapeados:
            try:
                self.cursor.execute(
                    "INSERT INTO evolucao_caso (descricao) VALUES (%s) ON CONFLICT (descricao) DO NOTHING",
                    (valor,)
                )
            except Exception as e:
                print(f"  ✗ Erro ao inserir '{valor}': {e}")
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM evolucao_caso")
        print(f"✓ Evolução do Caso: {self.cursor.fetchone()[0]} registros inseridos")
    
    def migrar_classificacao_final(self):
        """Migra tabela classificacao_final"""
        print("\n--- Migrando CLASSIFICAÇÃO FINAL ---")
        
        valores_unicos = self.df['classificacaoFinal'].dropna().unique()
        valores_mapeados = [self.mapear_valor('classificacao_final', v) for v in valores_unicos]
        valores_mapeados = list(set([v for v in valores_mapeados if v]))
        
        for valor in valores_mapeados:
            try:
                self.cursor.execute(
                    "INSERT INTO classificacao_final (descricao) VALUES (%s) ON CONFLICT (descricao) DO NOTHING",
                    (valor,)
                )
            except Exception as e:
                print(f"  ✗ Erro ao inserir '{valor}': {e}")
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM classificacao_final")
        print(f"✓ Classificação Final: {self.cursor.fetchone()[0]} registros inseridos")
    
    def migrar_sintomas(self):
        """Migra tabela sintoma - 'Outros' deve ser o 10º (sintoma_id = 10)"""
        print("\n--- Migrando SINTOMAS ---")
        
        # Campo sintomas contém múltiplos sintomas separados por vírgula
        todos_sintomas = []
        for sintomas_str in self.df['sintomas'].dropna().unique():
            if pd.notna(sintomas_str):
                sintomas_lista = [s.strip() for s in str(sintomas_str).split(',')]
                todos_sintomas.extend(sintomas_lista)
        
        valores_mapeados = [self.mapear_valor('sintoma', s) for s in set(todos_sintomas)]
        valores_unicos = sorted(list(set([v for v in valores_mapeados if v])))
        
        # Garantir que 'Outros' seja o 10º item da lista
        if 'Outros' in valores_unicos:
            valores_unicos.remove('Outros')
        
        # Inserir os primeiros 9 sintomas
        sintomas_ordenados = valores_unicos[:9]
        # Adicionar 'Outros' na 10ª posição
        sintomas_ordenados.append('Outros')
        # Adicionar o restante (se houver mais de 9)
        sintomas_ordenados.extend(valores_unicos[9:])
        
        for valor in sintomas_ordenados:
            try:
                self.cursor.execute(
                    "INSERT INTO sintoma (descricao) VALUES (%s) ON CONFLICT (descricao) DO NOTHING",
                    (valor,)
                )
            except Exception as e:
                print(f"  ✗ Erro ao inserir '{valor}': {e}")
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM sintoma")
        print(f"✓ Sintomas: {self.cursor.fetchone()[0]} registros inseridos (Outros = 10º)")
    
    def migrar_condicoes(self):
        """Migra tabela condicao - 'Outros' deve ser o 9º (condicao_id = 9)"""
        print("\n--- Migrando CONDIÇÕES ---")
        
        # Campo condicoes contém múltiplas condições separadas por vírgula
        # MAS algumas condições têm vírgulas internas (ex: "GRAUS 3, 4 OU 5")
        # Usar lista de condições válidas do mapeamento
        condicoes_validas = list(MAPEAMENTOS['condicao'].keys())
        
        todas_condicoes = set()
        for condicoes_str in self.df['condicoes'].dropna().unique():
            if pd.notna(condicoes_str):
                condicoes_str_upper = str(condicoes_str).upper()
                # Encontrar condições válidas na string
                for condicao_valida in condicoes_validas:
                    if condicao_valida in condicoes_str_upper:
                        todas_condicoes.add(condicao_valida)
        
        # Mapear valores para o formato do banco
        valores_mapeados = [self.mapear_valor('condicao', c) for c in todas_condicoes]
        valores_unicos = sorted(list(set([v for v in valores_mapeados if v])))
        
        # Garantir que 'Outros' seja o 9º item da lista
        if 'Outros' in valores_unicos:
            valores_unicos.remove('Outros')
        
        # Inserir as primeiras 8 condições
        condicoes_ordenadas = valores_unicos[:8]
        # Adicionar 'Outros' na 9ª posição
        condicoes_ordenadas.append('Outros')
        # Adicionar o restante (se houver mais de 8)
        condicoes_ordenadas.extend(valores_unicos[8:])
        
        for valor in condicoes_ordenadas:
            try:
                self.cursor.execute(
                    "INSERT INTO condicao (descricao) VALUES (%s) ON CONFLICT (descricao) DO NOTHING",
                    (valor,)
                )
            except Exception as e:
                print(f"  ✗ Erro ao inserir '{valor}': {e}")
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM condicao")
        print(f"✓ Condições: {self.cursor.fetchone()[0]} registros inseridos (Outros = 9º)")
    
    def migrar_tabelas_codigo(self):
        """Migra tabelas com códigos fixos (estrategia, local_testagem, resultado_teste, tipo_teste, estado_teste)"""
        print("\n--- Migrando TABELAS DE CÓDIGO ---")
        
        # Estratégia (1, 2, 3)
        for codigo in [1, 2, 3]:
            self.cursor.execute(
                "INSERT INTO estrategia (codigo) VALUES (%s) ON CONFLICT (codigo) DO NOTHING",
                (codigo,)
            )
        
        # Local de testagem (1-7)
        for codigo in range(1, 8):
            self.cursor.execute(
                "INSERT INTO local_testagem (codigo) VALUES (%s) ON CONFLICT (codigo) DO NOTHING",
                (codigo,)
            )
        
        # Resultado teste (1, 2, 3)
        for codigo in [1, 2, 3]:
            self.cursor.execute(
                "INSERT INTO resultado_teste (codigo) VALUES (%s) ON CONFLICT (codigo) DO NOTHING",
                (codigo,)
            )
        
        # Tipo teste (1-9)
        for codigo in range(1, 10):
            self.cursor.execute(
                "INSERT INTO tipo_teste (codigo) VALUES (%s) ON CONFLICT (codigo) DO NOTHING",
                (codigo,)
            )
        
        # Estado teste (1-4)
        for codigo in range(1, 5):
            self.cursor.execute(
                "INSERT INTO estado_teste (codigo) VALUES (%s) ON CONFLICT (codigo) DO NOTHING",
                (codigo,)
            )
        
        self.conn.commit()
        print("✓ Tabelas de código inseridas")
    
    def migrar_estado(self):
        """Migra tabela estado"""
        print("\n--- Migrando ESTADO ---")
        
        # Extrair estados únicos do campo 'estado'
        estados_unicos = self.df['estado'].dropna().unique()
        
        # Dicionário de estados com códigos IBGE (2 dígitos)
        estados_ibge = {
            'ACRE': '12', 'ALAGOAS': '27', 'AMAPÁ': '16', 'AMAPA': '16',
            'AMAZONAS': '13', 'BAHIA': '29', 'CEARÁ': '23', 'CEARA': '23',
            'DISTRITO FEDERAL': '53', 'ESPÍRITO SANTO': '32', 'ESPIRITO SANTO': '32',
            'GOIÁS': '52', 'GOIAS': '52', 'MARANHÃO': '21', 'MARANHAO': '21',
            'MATO GROSSO': '51', 'MATO GROSSO DO SUL': '50',
            'MINAS GERAIS': '31', 'PARÁ': '15', 'PARA': '15',
            'PARAÍBA': '25', 'PARAIBA': '25', 'PARANÁ': '41', 'PARANA': '41',
            'PERNAMBUCO': '26', 'PIAUÍ': '22', 'PIAUI': '22',
            'RIO DE JANEIRO': '33', 'RIO GRANDE DO NORTE': '24',
            'RIO GRANDE DO SUL': '43', 'RONDÔNIA': '11', 'RONDONIA': '11',
            'RORAIMA': '14', 'SANTA CATARINA': '42', 'SÃO PAULO': '35',
            'SAO PAULO': '35', 'SERGIPE': '28', 'TOCANTINS': '17'
        }
        
        for estado_nome in estados_unicos:
            estado_upper = str(estado_nome).strip().upper()
            codigo_ibge = estados_ibge.get(estado_upper)
            
            if codigo_ibge:
                # Formatar nome (primeira letra maiúscula)
                nome_formatado = estado_nome.strip().title()
                try:
                    self.cursor.execute(
                        "INSERT INTO estado (nome, codigo_ibge) VALUES (%s, %s) ON CONFLICT (codigo_ibge) DO NOTHING",
                        (nome_formatado, codigo_ibge)
                    )
                except Exception as e:
                    print(f"  ✗ Erro ao inserir '{nome_formatado}': {e}")
            else:
                print(f"  ⚠ Estado não mapeado: {estado_nome}")
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM estado")
        print(f"✓ Estados: {self.cursor.fetchone()[0]} registros inseridos")
    
    def migrar_cbo(self):
        """Migra tabela cbo (Classificação Brasileira de Ocupações)"""
        print("\n--- Migrando CBO ---")
        
        # Extrair CBOs únicos - campo 'cbo' contém formato "CODIGO - TITULO"
        cbos_unicos = self.df['cbo'].dropna().unique()
        
        for cbo_valor in cbos_unicos:
            cbo_str = str(cbo_valor).strip()
            
            # Ignorar valores inválidos
            if cbo_str.lower() == 'não informado' or not cbo_str:
                continue
            
            # Separar código e título (formato: "CODIGO - TITULO")
            if ' - ' in cbo_str:
                partes = cbo_str.split(' - ', 1)
                codigo = partes[0].strip()
                titulo = partes[1].strip() if len(partes) > 1 else f'CBO {codigo}'
            else:
                # Se não tem separador, assumir que é só o código
                codigo = cbo_str
                titulo = f'CBO {codigo}'
            
            # Validar código (deve ter até 6 dígitos)
            if not codigo.isdigit() or len(codigo) > 6:
                print(f"  ⚠ CBO ignorado (código inválido): {cbo_str}")
                continue
            
            # Padronizar código com 6 dígitos (completar com zeros à esquerda)
            codigo_padrao = codigo.zfill(6)
            
            try:
                self.cursor.execute(
                    "INSERT INTO cbo (codigo, titulo) VALUES (%s, %s) ON CONFLICT (codigo) DO NOTHING",
                    (codigo_padrao, titulo)
                )
            except Exception as e:
                print(f"  ✗ Erro ao inserir CBO '{codigo_padrao}': {e}")
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM cbo")
        print(f"✓ CBOs: {self.cursor.fetchone()[0]} registros inseridos")
    
    def migrar_laboratorio_vacina(self):
        """Migra tabela laboratorio_vacina"""
        print("\n--- Migrando LABORATÓRIO VACINA ---")
        
        # Extrair laboratórios dos campos de dose 1 e 2
        labs = set()
        
        for campo in ['codigoLaboratorioPrimeiraDose', 'codigoLaboratorioSegundaDose']:
            if campo in self.df.columns:
                valores = self.df[campo].dropna().unique()
                labs.update([str(v).strip() for v in valores if pd.notna(v) and str(v).strip()])
        
        for lab in sorted(labs):
            # Ignorar valores inválidos
            if lab.lower() == 'não informado' or not lab:
                continue
            
            try:
                self.cursor.execute(
                    "INSERT INTO laboratorio_vacina (nome) VALUES (%s) ON CONFLICT (nome) DO NOTHING",
                    (lab,)
                )
            except Exception as e:
                print(f"  ✗ Erro ao inserir laboratório '{lab}': {e}")
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM laboratorio_vacina")
        print(f"✓ Laboratórios: {self.cursor.fetchone()[0]} registros inseridos")
    
    def migrar_fabricante_teste(self):
        """Migra tabela fabricante_teste"""
        print("\n--- Migrando FABRICANTE TESTE ---")
        
        # Extrair fabricantes dos 4 campos de teste
        fabricantes = set()
        
        for i in range(1, 5):
            campo = f'codigoFabricanteTeste{i}'
            if campo in self.df.columns:
                valores = self.df[campo].dropna().unique()
                fabricantes.update([str(v).strip() for v in valores if pd.notna(v) and str(v).strip()])
        
        for fabricante_codigo in sorted(fabricantes):
            try:
                self.cursor.execute(
                    "INSERT INTO fabricante_teste (codigo) VALUES (%s) ON CONFLICT (codigo) DO NOTHING",
                    (fabricante_codigo,)
                )
            except Exception as e:
                print(f"  ✗ Erro ao inserir fabricante '{fabricante_codigo}': {e}")
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM fabricante_teste")
        print(f"✓ Fabricantes de teste: {self.cursor.fetchone()[0]} registros inseridos")
    
    def executar_migracao_fase1(self):
        """Executa a migração da Fase 1: Tabelas de Domínio"""
        print("="*60)
        print("INICIANDO MIGRAÇÃO - FASE 1: TABELAS DE DOMÍNIO")
        print("="*60)
        
        try:
            self.conectar()
            
            self.migrar_sexo()
            self.migrar_raca()
            self.migrar_evolucao_caso()
            self.migrar_classificacao_final()
            self.migrar_sintomas()
            self.migrar_condicoes()
            self.migrar_estado()
            self.migrar_cbo()
            self.migrar_laboratorio_vacina()
            self.migrar_fabricante_teste()
            self.migrar_tabelas_codigo()
            
            print("\n" + "="*60)
            print("✓ FASE 1 CONCLUÍDA COM SUCESSO")
            print("="*60)
            
        except Exception as e:
            print(f"\n✗ ERRO NA MIGRAÇÃO: {e}")
            self.conn.rollback()
            raise
        finally:
            self.desconectar()
    
    # ============= FASE 2: TABELAS COM DEPENDÊNCIA DE DOMÍNIO =============
    
    def migrar_municipio(self):
        """Migra tabela municipio (depende de estado)"""
        print("\n--- Migrando MUNICÍPIOS ---")
        
        # Extrair municípios únicos com seus estados e códigos IBGE
        df_municipios = self.df[['municipio', 'municipioIBGE', 'estado']].drop_duplicates()
        df_municipios = df_municipios.dropna(subset=['municipio', 'estado'])
        
        # Primeiro, precisamos mapear estado_id
        self.cursor.execute("SELECT estado_id, nome FROM estado")
        estados_map = {nome.upper(): estado_id for estado_id, nome in self.cursor.fetchall()}
        
        municipios_inseridos = 0
        municipios_erro = 0
        
        for _, row in df_municipios.iterrows():
            municipio_nome = str(row['municipio']).strip().title()
            estado_nome = str(row['estado']).strip().upper()
            codigo_ibge = str(int(row['municipioIBGE'])) if pd.notna(row['municipioIBGE']) else None
            
            # Buscar estado_id
            estado_id = estados_map.get(estado_nome)
            
            if not estado_id:
                print(f"  ⚠ Estado não encontrado: {estado_nome}")
                municipios_erro += 1
                continue
            
            try:
                if codigo_ibge:
                    self.cursor.execute(
                        "INSERT INTO municipio (nome, codigo_ibge, estado_id) VALUES (%s, %s, %s) ON CONFLICT (codigo_ibge) DO NOTHING",
                        (municipio_nome, codigo_ibge, estado_id)
                    )
                else:
                    # Se não tem código IBGE, inserir apenas com nome e estado
                    self.cursor.execute(
                        "INSERT INTO municipio (nome, estado_id) SELECT %s, %s WHERE NOT EXISTS (SELECT 1 FROM municipio WHERE nome = %s AND estado_id = %s)",
                        (municipio_nome, estado_id, municipio_nome, estado_id)
                    )
                
                if self.cursor.rowcount > 0:
                    municipios_inseridos += 1
                    
            except Exception as e:
                print(f"  ✗ Erro ao inserir município '{municipio_nome}' ({estado_nome}): {e}")
                municipios_erro += 1
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM municipio")
        print(f"✓ Municípios: {self.cursor.fetchone()[0]} registros no banco ({municipios_inseridos} novos inseridos, {municipios_erro} erros)")
    
    # ============= FASE 3: ENTIDADES PRINCIPAIS =============
    
    def migrar_paciente(self):
        """Migra tabela paciente (depende de sexo, raca)"""
        print("\n--- Migrando PACIENTES ---")
        
        # Buscar mapeamentos de sexo e raça
        self.cursor.execute("SELECT sexo_id, descricao FROM sexo")
        sexo_map = {desc.upper(): sexo_id for sexo_id, desc in self.cursor.fetchall()}
        
        self.cursor.execute("SELECT raca_id, descricao FROM raca")
        raca_map = {desc.upper(): raca_id for raca_id, desc in self.cursor.fetchall()}
        
        # Preparar dados de pacientes únicos
        # Cada combinação única de (idade, sexo, raça, comunidade tradicional) é um paciente
        df_pacientes = self.df[['idade', 'sexo', 'racaCor', 'codigoContemComunidadeTradicional']].copy()
        df_pacientes = df_pacientes.drop_duplicates()
        
        pacientes_inseridos = 0
        pacientes_erro = 0
        
        for _, row in df_pacientes.iterrows():
            idade = int(row['idade']) if pd.notna(row['idade']) else None
            sexo_str = str(row['sexo']).strip().upper()
            raca_str = str(row['racaCor']).strip().upper()
            
            # Mapear membro_povo_tradicional (já vem como True/False/NaN)
            membro_povo_tradicional = row['codigoContemComunidadeTradicional'] if pd.notna(row['codigoContemComunidadeTradicional']) else False
            
            # Buscar IDs
            sexo_id = sexo_map.get(sexo_str)
            raca_id = raca_map.get(raca_str)
            
            if not sexo_id or not raca_id:
                pacientes_erro += 1
                continue
            
            try:
                self.cursor.execute(
                    """INSERT INTO paciente (idade, sexo_id, raca_id, membro_povo_tradicional) 
                       VALUES (%s, %s, %s, %s)""",
                    (idade, sexo_id, raca_id, membro_povo_tradicional)
                )
                pacientes_inseridos += 1
                
            except Exception as e:
                print(f"  ✗ Erro ao inserir paciente: {e}")
                pacientes_erro += 1
                
                if pacientes_erro > 10:
                    print("  ⚠ Muitos erros, parando inserção de pacientes")
                    break
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM paciente")
        print(f"✓ Pacientes: {self.cursor.fetchone()[0]} registros inseridos ({pacientes_erro} erros)")
    
    # ============= FASE 4: NÚCLEO CENTRAL =============
    
    def migrar_notificacao(self):
        """Migra tabela notificacao (depende de paciente, municipio, cbo)"""
        print("\n--- Migrando NOTIFICAÇÕES ---")
        
        # Buscar mapeamentos necessários
        self.cursor.execute("SELECT cbo_id, codigo FROM cbo")
        cbo_map = {codigo: cbo_id for cbo_id, codigo in self.cursor.fetchall()}
        
        # Criar cache de paciente_id baseado em combinação de atributos
        print("  Criando cache de pacientes...")
        pacientes_cache = {}
        self.cursor.execute("""
            SELECT paciente_id, idade, sexo_id, raca_id, membro_povo_tradicional 
            FROM paciente
        """)
        for pac_id, idade, sexo_id, raca_id, membro_trad in self.cursor.fetchall():
            key = (idade, sexo_id, raca_id, membro_trad)
            pacientes_cache[key] = pac_id
        
        # Criar cache de município_id
        print("  Criando cache de municípios...")
        municipios_cache = {}
        self.cursor.execute("""
            SELECT m.municipio_id, m.nome, e.nome as estado_nome
            FROM municipio m
            JOIN estado e ON m.estado_id = e.estado_id
        """)
        for mun_id, mun_nome, est_nome in self.cursor.fetchall():
            key = (mun_nome.upper(), est_nome.upper())
            municipios_cache[key] = mun_id
        
        # Mapear sexo e raça para buscar paciente_id
        self.cursor.execute("SELECT sexo_id, descricao FROM sexo")
        sexo_map = {desc.upper(): sexo_id for sexo_id, desc in self.cursor.fetchall()}
        
        self.cursor.execute("SELECT raca_id, descricao FROM raca")
        raca_map = {desc.upper(): raca_id for raca_id, desc in self.cursor.fetchall()}
        
        notificacoes_inseridas = 0
        notificacoes_erro = 0
        
        print("  Inserindo notificações...")
        for idx, row in self.df.iterrows():
            # Buscar paciente_id
            idade = int(row['idade']) if pd.notna(row['idade']) else None
            sexo_id = sexo_map.get(str(row['sexo']).strip().upper())
            raca_id = raca_map.get(str(row['racaCor']).strip().upper())
            membro_trad = row['codigoContemComunidadeTradicional'] if pd.notna(row['codigoContemComunidadeTradicional']) else False
            
            paciente_key = (idade, sexo_id, raca_id, membro_trad)
            paciente_id = pacientes_cache.get(paciente_key)
            
            if not paciente_id:
                notificacoes_erro += 1
                continue
            
            # Buscar municipio_id de notificação
            mun_not_nome = str(row['municipioNotificacao']).strip().upper()
            est_not_nome = str(row['estadoNotificacao']).strip().upper()
            municipio_key = (mun_not_nome, est_not_nome)
            municipio_id = municipios_cache.get(municipio_key)
            
            if not municipio_id:
                notificacoes_erro += 1
                continue
            
            # Processar profissional_saude PRIMEIRO (necessário para validar CBO)
            prof_saude_str = str(row['profissionalSaude']).strip().upper()
            if prof_saude_str in ['TRUE', 'VERDADEIRO', 'SIM']:
                profissional_saude = True
            elif prof_saude_str in ['FALSE', 'FALSO', 'NÃO', 'NAO']:
                profissional_saude = False
            else:
                profissional_saude = False  # Default
            
            # Buscar CBO (opcional) - MAS obrigatório se profissional_saude = TRUE
            cbo_str = str(row['cbo']).strip()
            cbo_id = None
            if cbo_str and cbo_str.lower() != 'não informado' and ' - ' in cbo_str:
                codigo_cbo = cbo_str.split(' - ')[0].strip().zfill(6)
                cbo_id = cbo_map.get(codigo_cbo)
            
            # VALIDAR CONSTRAINT: profissional_saude = TRUE exige cbo_id NOT NULL
            # profissional_saude = FALSE exige cbo_id NULL
            if profissional_saude and not cbo_id:
                # Profissional de saúde mas sem CBO válido - pular registro
                notificacoes_erro += 1
                continue
            elif not profissional_saude:
                # Não é profissional de saúde - forçar cbo_id = NULL
                cbo_id = None
            
            # Processar profissional_seguranca
            prof_seg_str = str(row['profissionalSeguranca']).strip().upper()
            if prof_seg_str in ['TRUE', 'VERDADEIRO', 'SIM']:
                profissional_seguranca = True
            elif prof_seg_str in ['FALSE', 'FALSO', 'NÃO', 'NAO']:
                profissional_seguranca = False
            else:
                profissional_seguranca = None
            
            # Data de notificação
            data_notificacao = row['dataNotificacao']
            if pd.isna(data_notificacao):
                notificacoes_erro += 1
                continue
            
            # Origem
            origem = str(row['origem']).strip() if pd.notna(row['origem']) and str(row['origem']).strip().upper() != 'NÃO INFORMADO' else None
            
            # Excluído e Validado
            excluido = row['excluido'] if pd.notna(row['excluido']) else None
            
            validado_str = str(row['validado']).strip().upper()
            if validado_str in ['TRUE', 'VERDADEIRO', 'SIM']:
                validado = True
            elif validado_str in ['FALSE', 'FALSO', 'NÃO', 'NAO']:
                validado = False
            else:
                validado = None
            
            try:
                self.cursor.execute(
                    """INSERT INTO notificacao (paciente_id, municipio_notificacao_id, cbo_id, 
                       profissional_saude, profissional_seguranca, data_notificacao, origem, excluido, validado)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                       RETURNING notificacao_id""",
                    (paciente_id, municipio_id, cbo_id, profissional_saude, profissional_seguranca,
                     data_notificacao, origem, excluido, validado)
                )
                notificacoes_inseridas += 1
                
                # Commit a cada 1000 registros
                if notificacoes_inseridas % 1000 == 0:
                    self.conn.commit()
                    print(f"  → {notificacoes_inseridas} notificações inseridas...")
                    
            except Exception as e:
                print(f"  ✗ Erro ao inserir notificação (linha {idx}): {e}")
                notificacoes_erro += 1
                
                if notificacoes_erro > 50:
                    print("  ⚠ Muitos erros, parando inserção")
                    break
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM notificacao")
        print(f"✓ Notificações: {self.cursor.fetchone()[0]} registros inseridos ({notificacoes_erro} erros)")
    
    # ============= FASE 5: TABELAS DEPENDENTES DE NOTIFICAÇÃO =============
    
    def migrar_residencia_paciente(self, notificacao_paciente_map):
        """Migra tabela residencia_paciente (relação paciente-município de residência)"""
        print("\n--- Migrando RESIDÊNCIA PACIENTE ---")
        
        # Criar cache de municípios
        self.cursor.execute("""
            SELECT m.municipio_id, m.nome, e.nome as estado_nome
            FROM municipio m
            JOIN estado e ON m.estado_id = e.estado_id
        """)
        municipios_cache = {}
        for mun_id, mun_nome, est_nome in self.cursor.fetchall():
            key = (mun_nome.upper(), est_nome.upper())
            municipios_cache[key] = mun_id
        
        residencias_inseridas = 0
        residencias_erro = 0
        residencias_vistas = set()
        
        for idx, row in self.df.iterrows():
            # Buscar município de residência
            mun_res_nome = str(row['municipio']).strip().upper()
            est_res_nome = str(row['estado']).strip().upper()
            municipio_key = (mun_res_nome, est_res_nome)
            municipio_id = municipios_cache.get(municipio_key)
            
            # Buscar paciente_id via notificação
            if idx not in notificacao_paciente_map:
                continue
            
            paciente_id = notificacao_paciente_map[idx]
            
            if not municipio_id or not paciente_id:
                residencias_erro += 1
                continue
            
            # Evitar duplicatas (mesma combinação paciente-município)
            key = (paciente_id, municipio_id)
            if key in residencias_vistas:
                continue
            residencias_vistas.add(key)
            
            try:
                self.cursor.execute(
                    """INSERT INTO residencia_paciente (paciente_id, municipio_id)
                       VALUES (%s, %s) ON CONFLICT (paciente_id, municipio_id) DO NOTHING""",
                    (paciente_id, municipio_id)
                )
                if self.cursor.rowcount > 0:
                    residencias_inseridas += 1
            except Exception as e:
                print(f"  ✗ Erro ao inserir residência (linha {idx}): {e}")
                self.conn.rollback()  # CRITICAL: desfazer transação abortada
                residencias_erro += 1
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM residencia_paciente")
        print(f"✓ Residências: {self.cursor.fetchone()[0]} registros ({residencias_erro} erros)")
    
    def migrar_dados_clinicos(self, notificacao_id_map):
        """Migra tabela dados_clinicos"""
        print("\n--- Migrando DADOS CLÍNICOS ---")
        
        # Buscar mapeamentos
        self.cursor.execute("SELECT classificacao_final_id, descricao FROM classificacao_final")
        classif_map = {desc.upper(): id for id, desc in self.cursor.fetchall()}
        
        self.cursor.execute("SELECT evolucao_caso_id, descricao FROM evolucao_caso")
        evolucao_map = {desc.upper(): id for id, desc in self.cursor.fetchall()}
        
        dados_inseridos = 0
        dados_erro = 0
        
        for idx, row in self.df.iterrows():
            if idx not in notificacao_id_map:
                continue
            
            notificacao_id = notificacao_id_map[idx]
            
            # Data início sintomas (opcional agora)
            data_inicio_sintomas = row['dataInicioSintomas'] if pd.notna(row['dataInicioSintomas']) else None
            
            # Classificação final
            classif_str = str(row['classificacaoFinal']).strip().upper()
            classif_mapeado = self.mapear_valor('classificacao_final', classif_str)
            classificacao_final_id = classif_map.get(classif_mapeado) if classif_mapeado else None
            
            # Evolução caso
            evolucao_str = str(row['evolucaoCaso']).strip().upper()
            evolucao_mapeado = self.mapear_valor('evolucao_caso', evolucao_str)
            evolucao_caso_id = evolucao_map.get(evolucao_mapeado) if evolucao_mapeado else None
            
            # Data encerramento - VALIDAR: deve ser >= data_inicio_sintomas
            data_encerramento = row['dataEncerramento'] if pd.notna(row['dataEncerramento']) else None
            
            # VALIDAR CONSTRAINT chk_data_encerramento:
            # - Se data_encerramento < data_inicio_sintomas → forçar NULL (só se ambas existirem)
            # - Se data_encerramento existe MAS evolucao_caso_id é NULL → forçar data_encerramento NULL
            # - Se evolucao_caso_id existe MAS data_encerramento é NULL → forçar evolucao_caso_id NULL
            if data_encerramento and data_inicio_sintomas and data_encerramento < data_inicio_sintomas:
                data_encerramento = None
                evolucao_caso_id = None
            elif data_encerramento and not evolucao_caso_id:
                # Tem data mas não tem evolução → forçar data NULL
                data_encerramento = None
            elif evolucao_caso_id and not data_encerramento:
                # Tem evolução mas não tem data → forçar evolução NULL
                evolucao_caso_id = None
            
            # Especificação outros sintomas/condições
            espec_outros_sintomas = str(row['outrosSintomas']).strip() if pd.notna(row['outrosSintomas']) and str(row['outrosSintomas']).strip().upper() != 'NÃO INFORMADO' else None
            espec_outras_condicoes = str(row['outrasCondicoes']).strip() if pd.notna(row['outrasCondicoes']) and str(row['outrasCondicoes']).strip().upper() != 'NÃO INFORMADO' else None
            
            # Código recebeu vacina (1, 2, 3)
            codigo_recebeu_vacina = int(row['codigoRecebeuVacina']) if pd.notna(row['codigoRecebeuVacina']) else None
            
            try:
                self.cursor.execute(
                    """INSERT INTO dados_clinicos (notificacao_id, data_inicio_sintomas, classificacao_final_id,
                       evolucao_caso_id, especificacao_outros_sintomas, especificacao_outras_condicoes,
                       codigo_recebeu_vacina, data_encerramento)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (notificacao_id) DO NOTHING""",
                    (notificacao_id, data_inicio_sintomas, classificacao_final_id, evolucao_caso_id,
                     espec_outros_sintomas, espec_outras_condicoes, codigo_recebeu_vacina, data_encerramento)
                )
                dados_inseridos += 1
            except Exception as e:
                print(f"  ✗ Erro ao inserir dados clínicos (linha {idx}): {e}")
                self.conn.rollback()
                dados_erro += 1
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM dados_clinicos")
        print(f"✓ Dados clínicos: {self.cursor.fetchone()[0]} registros ({dados_erro} erros)")
    
    def migrar_notificacao_sintoma(self, notificacao_id_map):
        """Migra tabela notificacao_sintoma (relação N:N)"""
        print("\n--- Migrando NOTIFICAÇÃO-SINTOMA ---")
        
        # Buscar mapeamento de sintomas
        self.cursor.execute("SELECT sintoma_id, descricao FROM sintoma")
        sintoma_map = {desc.upper(): id for id, desc in self.cursor.fetchall()}
        
        relacoes_inseridas = 0
        relacoes_erro = 0
        
        for idx, row in self.df.iterrows():
            if idx not in notificacao_id_map:
                continue
            
            notificacao_id = notificacao_id_map[idx]
            sintomas_str = str(row['sintomas']).strip()
            
            if not sintomas_str or sintomas_str.upper() == 'NÃO INFORMADO':
                continue
            
            # Split por vírgula
            sintomas_lista = [s.strip() for s in sintomas_str.split(',')]
            
            for sintoma_nome in sintomas_lista:
                sintoma_mapeado = self.mapear_valor('sintoma', sintoma_nome)
                sintoma_id = sintoma_map.get(sintoma_mapeado.upper()) if sintoma_mapeado else None
                
                if not sintoma_id:
                    continue
                
                try:
                    self.cursor.execute(
                        """INSERT INTO notificacao_sintoma (notificacao_id, sintoma_id)
                           VALUES (%s, %s) ON CONFLICT (notificacao_id, sintoma_id) DO NOTHING""",
                        (notificacao_id, sintoma_id)
                    )
                    if self.cursor.rowcount > 0:
                        relacoes_inseridas += 1
                except Exception as e:
                    print(f"  ✗ Erro ao inserir sintoma (linha {idx}): {e}")
                    self.conn.rollback()
                    relacoes_erro += 1
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM notificacao_sintoma")
        print(f"✓ Notificação-Sintoma: {self.cursor.fetchone()[0]} registros ({relacoes_erro} erros)")
    
    def migrar_notificacao_condicao(self, notificacao_id_map):
        """Migra tabela notificacao_condicao (relação N:N)"""
        print("\n--- Migrando NOTIFICAÇÃO-CONDIÇÃO ---")
        
        # Buscar mapeamento de condições
        self.cursor.execute("SELECT condicao_id, descricao FROM condicao")
        condicao_map = {desc.upper(): id for id, desc in self.cursor.fetchall()}
        
        # Criar lista de condições válidas ordenadas por tamanho (maior primeiro)
        # para evitar match parcial (ex: "DIABETES" não pode casar com "DIABETES, OUTROS")
        condicoes_validas = sorted(MAPEAMENTOS['condicao'].keys(), key=len, reverse=True)
        
        relacoes_inseridas = 0
        relacoes_erro = 0
        
        for idx, row in self.df.iterrows():
            if idx not in notificacao_id_map:
                continue
            
            notificacao_id = notificacao_id_map[idx]
            condicoes_str = str(row['condicoes']).strip().upper()
            
            if not condicoes_str or condicoes_str == 'NÃO INFORMADO':
                continue
            
            # Identificar condições presentes na string usando match de substrings
            condicoes_encontradas = []
            condicoes_str_temp = condicoes_str
            
            for condicao_valida in condicoes_validas:
                if condicao_valida in condicoes_str_temp:
                    condicoes_encontradas.append(condicao_valida)
                    # Remove a condição já encontrada para evitar duplicatas
                    condicoes_str_temp = condicoes_str_temp.replace(condicao_valida, '', 1)
            
            # Inserir cada condição encontrada
            for condicao_nome in condicoes_encontradas:
                condicao_mapeada = self.mapear_valor('condicao', condicao_nome)
                condicao_id = condicao_map.get(condicao_mapeada.upper()) if condicao_mapeada else None
                
                if not condicao_id:
                    continue
                
                try:
                    self.cursor.execute(
                        """INSERT INTO notificacao_condicao (notificacao_id, condicao_id)
                           VALUES (%s, %s) ON CONFLICT (notificacao_id, condicao_id) DO NOTHING""",
                        (notificacao_id, condicao_id)
                    )
                    if self.cursor.rowcount > 0:
                        relacoes_inseridas += 1
                except Exception as e:
                    print(f"  ✗ Erro ao inserir condição (linha {idx}): {e}")
                    self.conn.rollback()
                    relacoes_erro += 1
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM notificacao_condicao")
        print(f"✓ Notificação-Condição: {self.cursor.fetchone()[0]} registros ({relacoes_erro} erros)")
    
    def migrar_notificacao_vacina(self, notificacao_id_map):
        """Migra tabela notificacao_vacina (doses de vacina)"""
        print("\n--- Migrando NOTIFICAÇÃO VACINA ---")
        
        # Buscar mapeamento de laboratórios
        self.cursor.execute("SELECT laboratorio_vacina_id, nome FROM laboratorio_vacina")
        lab_map = {nome.upper(): id for id, nome in self.cursor.fetchall()}
        
        vacinas_inseridas = 0
        vacinas_erro = 0
        
        for idx, row in self.df.iterrows():
            if idx not in notificacao_id_map:
                continue
            
            notificacao_id = notificacao_id_map[idx]
            
            # Processar codigoDosesVacina (multivalorado: "1,2", "2,3", "1,2,3,4", etc.)
            codigo_doses = str(row['codigoDosesVacina']).strip()
            
            if not codigo_doses or codigo_doses in ['nan', 'None', 'NÃO INFORMADO']:
                continue
            
            # Split para obter lista de doses
            doses_lista = [int(d.strip()) for d in codigo_doses.split(',') if d.strip().isdigit()]
            
            # Mapear doses para campos do dataset
            # dataPrimeiraDose/dataSegundaDose são ordem cronológica, não número da dose
            # Vamos inserir baseado na ordem cronológica que temos dados
            
            doses_inserir = []
            
            # Primeira dose cronológica (se existe data)
            if pd.notna(row['dataPrimeiraDose']):
                lab1_nome = str(row['codigoLaboratorioPrimeiraDose']).strip().upper()
                lab1_id = lab_map.get(lab1_nome) if lab1_nome != 'NÃO INFORMADO' else None
                lote1 = str(row['lotePrimeiraDose']).strip() if pd.notna(row['lotePrimeiraDose']) and str(row['lotePrimeiraDose']).strip().upper() != 'NÃO INFORMADO' else None
                
                # Determinar número da dose: se doses_lista tem valores, usar o primeiro
                dose_num = doses_lista[0] if doses_lista else 1
                
                doses_inserir.append({
                    'dose_numero': dose_num,
                    'data_vacinacao': row['dataPrimeiraDose'],
                    'lab_id': lab1_id,
                    'lote': lote1
                })
            
            # Segunda dose cronológica (se existe data)
            if pd.notna(row['dataSegundaDose']):
                lab2_nome = str(row['codigoLaboratorioSegundaDose']).strip().upper()
                lab2_id = lab_map.get(lab2_nome) if lab2_nome != 'NÃO INFORMADO' else None
                lote2 = str(row['loteSegundaDose']).strip() if pd.notna(row['loteSegundaDose']) and str(row['loteSegundaDose']).strip().upper() != 'NÃO INFORMADO' else None
                
                # Determinar número da dose: se doses_lista tem 2+ valores, usar o segundo, senão usar 2
                dose_num = doses_lista[1] if len(doses_lista) > 1 else 2
                
                doses_inserir.append({
                    'dose_numero': dose_num,
                    'data_vacinacao': row['dataSegundaDose'],
                    'lab_id': lab2_id,
                    'lote': lote2
                })
            
            # Inserir cada dose
            for dose_info in doses_inserir:
                try:
                    self.cursor.execute(
                        """INSERT INTO notificacao_vacina (notificacao_id, dose_numero, data_vacinacao, lab_id, lote)
                           VALUES (%s, %s, %s, %s, %s)""",
                        (notificacao_id, dose_info['dose_numero'], dose_info['data_vacinacao'], 
                         dose_info['lab_id'], dose_info['lote'])
                    )
                    vacinas_inseridas += 1
                except Exception as e:
                    print(f"  ✗ Erro ao inserir vacina (linha {idx}, dose {dose_info['dose_numero']}): {e}")
                    self.conn.rollback()
                    vacinas_erro += 1
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM notificacao_vacina")
        print(f"✓ Notificação Vacina: {self.cursor.fetchone()[0]} registros ({vacinas_erro} erros)")
    
    def migrar_teste_laboratorial(self, notificacao_id_map):
        """Migra tabela teste_laboratorial (até 4 testes por notificação)"""
        print("\n--- Migrando TESTE LABORATORIAL ---")
        
        # Buscar mapeamento de fabricantes
        self.cursor.execute("SELECT fabricante_id, codigo FROM fabricante_teste")
        fab_map = {codigo: id for id, codigo in self.cursor.fetchall()}
        
        testes_inseridos = 0
        testes_erro = 0
        
        for idx, row in self.df.iterrows():
            if idx not in notificacao_id_map:
                continue
            
            notificacao_id = notificacao_id_map[idx]
            
            # Processar até 4 testes
            for i in range(1, 5):
                resultado = int(row[f'codigoResultadoTeste{i}']) if pd.notna(row[f'codigoResultadoTeste{i}']) else None
                tipo = int(row[f'codigoTipoTeste{i}']) if pd.notna(row[f'codigoTipoTeste{i}']) else None
                estado = int(row[f'codigoEstadoTeste{i}']) if pd.notna(row[f'codigoEstadoTeste{i}']) else None
                data_coleta = row[f'dataColetaTeste{i}'] if pd.notna(row[f'dataColetaTeste{i}']) else None
                
                fab_codigo = str(row[f'codigoFabricanteTeste{i}']).strip() if pd.notna(row[f'codigoFabricanteTeste{i}']) and str(row[f'codigoFabricanteTeste{i}']).strip().upper() != 'NÃO INFORMADO' else None
                fabricante_id = fab_map.get(fab_codigo) if fab_codigo else None
                
                # Se não tem tipo ou estado, pular
                if not tipo or not estado:
                    continue
                
                # VALIDAR CONSTRAINT chk_estado_teste:
                # estado_id = 1: resultado, fabricante e data_coleta devem ser NULL
                # estado_id = 2: data_coleta deve ser NOT NULL
                # estado_id = 3: resultado e data_coleta devem ser NOT NULL
                # estado_id = 4: resultado, fabricante e data_coleta devem ser NULL
                
                # Ajustar campos baseado no estado para garantir conformidade
                if estado == 1:
                    resultado = None
                    fabricante_id = None
                    data_coleta = None
                elif estado == 2:
                    if not data_coleta:
                        # Estado 2 exige data_coleta - pular registro
                        continue
                elif estado == 3:
                    if not resultado or not data_coleta:
                        # Estado 3 exige resultado e data_coleta - pular registro
                        continue
                elif estado == 4:
                    resultado = None
                    fabricante_id = None
                    data_coleta = None
                
                try:
                    self.cursor.execute(
                        """INSERT INTO teste_laboratorial (notificacao_id, resultado_id, fabricante_id, 
                           tipo_id, estado_id, data_coleta)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        (notificacao_id, resultado, fabricante_id, tipo, estado, data_coleta)
                    )
                    testes_inseridos += 1
                except Exception as e:
                    print(f"  ✗ Erro ao inserir teste (linha {idx}): {e}")
                    self.conn.rollback()
                    testes_erro += 1
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM teste_laboratorial")
        print(f"✓ Testes Laboratoriais: {self.cursor.fetchone()[0]} registros ({testes_erro} erros)")
    
    def migrar_dados_estrategia_local(self, notificacao_id_map):
        """Migra tabela dados_estrategia_local_testagem"""
        print("\n--- Migrando DADOS ESTRATÉGIA/LOCAL TESTAGEM ---")
        
        dados_inseridos = 0
        dados_erro = 0
        
        for idx, row in self.df.iterrows():
            if idx not in notificacao_id_map:
                continue
            
            notificacao_id = notificacao_id_map[idx]
            
            # Local de testagem (obrigatório)
            local_testagem_id = int(row['codigoLocalRealizacaoTestagem']) if pd.notna(row['codigoLocalRealizacaoTestagem']) else None
            if not local_testagem_id:
                continue
            
            # Estratégia (opcional)
            estrategia_id = int(row['codigoEstrategiaCovid']) if pd.notna(row['codigoEstrategiaCovid']) else None
            
            # Especificação outro testagem
            espec_outro_testagem = str(row['outroLocalRealizacaoTestagem']).strip() if pd.notna(row['outroLocalRealizacaoTestagem']) and str(row['outroLocalRealizacaoTestagem']).strip().upper() != 'NÃO INFORMADO' else None
            
            # Código busca ativa
            codigo_busca_ativa = int(row['codigoBuscaAtivaAssintomatico']) if pd.notna(row['codigoBuscaAtivaAssintomatico']) else None
            espec_outro_ba = str(row['outroBuscaAtivaAssintomatico']).strip() if pd.notna(row['outroBuscaAtivaAssintomatico']) and str(row['outroBuscaAtivaAssintomatico']).strip().upper() != 'NÃO INFORMADO' else None
            
            # Código triagem específica
            codigo_triagem = int(row['codigoTriagemPopulacaoEspecifica']) if pd.notna(row['codigoTriagemPopulacaoEspecifica']) else None
            espec_outro_te = str(row['outroTriagemPopulacaoEspecifica']).strip() if pd.notna(row['outroTriagemPopulacaoEspecifica']) and str(row['outroTriagemPopulacaoEspecifica']).strip().upper() != 'NÃO INFORMADO' else None
            
            # VALIDAR CONSTRAINTS:
            # chk_busca_ativa: se estrategia_id != 2, codigo_busca_ativa deve ser NULL
            if estrategia_id != 2:
                codigo_busca_ativa = None
                espec_outro_ba = None
            
            # chk_triagem_especifica: se estrategia_id != 3, codigo_triagem deve ser NULL
            if estrategia_id != 3:
                codigo_triagem = None
                espec_outro_te = None
            
            # chk_outro_busca_ativa: se codigo_busca_ativa != 4, especificacao_outro_ba deve ser NULL
            if codigo_busca_ativa != 4:
                espec_outro_ba = None
            elif codigo_busca_ativa == 4 and not espec_outro_ba:
                # Código 4 exige especificação - forçar NULL para evitar erro
                codigo_busca_ativa = None
            
            # chk_outro_triagem_especifica: se codigo_triagem != 5, especificacao_outro_te deve ser NULL
            if codigo_triagem != 5:
                espec_outro_te = None
            elif codigo_triagem == 5 and not espec_outro_te:
                # Código 5 exige especificação - forçar NULL para evitar erro
                codigo_triagem = None
            
            # chk_outro_testagem: se local_testagem_id != 7, especificacao_outro_testagem deve ser NULL
            if local_testagem_id != 7:
                espec_outro_testagem = None
            elif local_testagem_id == 7 and not espec_outro_testagem:
                # Local 7 exige especificação - pular registro
                continue
            
            try:
                self.cursor.execute(
                    """INSERT INTO dados_estrategia_local_testagem (notificacao_id, local_testagem_id,
                       especificacao_outro_testagem, estrategia_id, codigo_busca_ativa, codigo_triagem_especifica,
                       especificacao_outro_ba, especificacao_outro_te)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (notificacao_id) DO NOTHING""",
                    (notificacao_id, local_testagem_id, espec_outro_testagem, estrategia_id,
                     codigo_busca_ativa, codigo_triagem, espec_outro_ba, espec_outro_te)
                )
                dados_inseridos += 1
            except Exception as e:
                print(f"  ✗ Erro ao inserir estratégia/local (linha {idx}): {e}")
                self.conn.rollback()
                dados_erro += 1
        
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM dados_estrategia_local_testagem")
        print(f"✓ Dados Estratégia/Local: {self.cursor.fetchone()[0]} registros ({dados_erro} erros)")
    
    def executar_migracao_fase5(self):
        """Executa migração da Fase 5: Tabelas dependentes de notificação"""
        print("\n" + "="*60)
        print("INICIANDO FASE 5: TABELAS DEPENDENTES DE NOTIFICAÇÃO")
        print("="*60)
        
        try:
            self.conectar()
            
            # Criar mapeamentos notificacao_id -> paciente_id e idx -> notificacao_id
            print("  Criando mapeamentos de notificação...")
            self.cursor.execute("SELECT notificacao_id, paciente_id FROM notificacao ORDER BY notificacao_id")
            notificacoes_db = self.cursor.fetchall()
            
            # Mapear índice do DataFrame -> notificacao_id (assumindo ordem de inserção)
            notificacao_id_map = {}
            notificacao_paciente_map = {}
            
            for i, (notif_id, pac_id) in enumerate(notificacoes_db):
                notificacao_id_map[i] = notif_id
                notificacao_paciente_map[i] = pac_id
            
            self.migrar_residencia_paciente(notificacao_paciente_map)
            self.migrar_dados_clinicos(notificacao_id_map)
            self.migrar_notificacao_sintoma(notificacao_id_map)
            self.migrar_notificacao_condicao(notificacao_id_map)
            self.migrar_notificacao_vacina(notificacao_id_map)
            self.migrar_teste_laboratorial(notificacao_id_map)
            self.migrar_dados_estrategia_local(notificacao_id_map)
            
            print("\n" + "="*60)
            print("✓ FASE 5 CONCLUÍDA COM SUCESSO")
            print("="*60)
            
        except Exception as e:
            print(f"\n✗ ERRO NA MIGRAÇÃO: {e}")
            self.conn.rollback()
            raise
        finally:
            self.desconectar()
    
    def executar_migracao_fase2_3_4(self):
        """Executa migração das Fases 2, 3 e 4"""
        print("\n" + "="*60)
        print("INICIANDO FASES 2, 3 e 4")
        print("="*60)
        
        try:
            self.conectar()
            
            # Fase 2
            print("\n" + "="*60)
            print("FASE 2: TABELAS COM DEPENDÊNCIA DE DOMÍNIO")
            print("="*60)
            self.migrar_municipio()
            
            # Fase 3
            print("\n" + "="*60)
            print("FASE 3: ENTIDADES PRINCIPAIS")
            print("="*60)
            self.migrar_paciente()
            
            # Fase 4
            print("\n" + "="*60)
            print("FASE 4: NÚCLEO CENTRAL")
            print("="*60)
            self.migrar_notificacao()
            
            print("\n" + "="*60)
            print("✓ FASES 2, 3 e 4 CONCLUÍDAS COM SUCESSO")
            print("="*60)
            
        except Exception as e:
            print(f"\n✗ ERRO NA MIGRAÇÃO: {e}")
            self.conn.rollback()
            raise
        finally:
            self.desconectar()


if __name__ == "__main__":
    migrador = MigradorDadosSUS('datasus_limpo.parquet', DB_CONFIG)
    migrador.executar_migracao_fase1()
    input("\nPressione Enter para iniciar as Fases 2, 3 e 4...")
    migrador.executar_migracao_fase2_3_4()
    input("\nPressione Enter para iniciar a Fase 5...")
    migrador.executar_migracao_fase5()