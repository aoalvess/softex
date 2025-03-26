import pandas as pd
import numpy as np
import unicodedata, re, dateparser, requests, psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine, text
from io import StringIO, BytesIO

# Carregar dados do Google Sheets
# https://docs.google.com/spreadsheets/d/1sH2xZoKWklZWirYo1K9EEg167CQV55bbh-ycH3poZPQ/export?format=csv&id=1sH2xZoKWklZWirYo1K9EEg167CQV55bbh-ycH3poZPQ&gid=0
sheet_id = "1sH2xZoKWklZWirYo1K9EEg167CQV55bbh-ycH3poZPQ"
gid = 0
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
#df = pd.read_excel(url)
df = pd.read_csv(url)
df = df.dropna(how='all')

###############################################################################################
# DEFINIÇÃO DE FUNÇÕES PARA TRATAMENTO DE DADOS
###############################################################################################

# Função genérica para conversão de datas
def tratar_coluna_data(df, colunas, formato_saida='%d/%m/%Y'):
    for col in colunas:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime(formato_saida)
            df[col] = df[col].replace('NaT', np.nan)
    return df

def tratar_coluna_dataparser(df, colunas, formato_saida='%d/%m/%Y'):
    for col in colunas:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: _parse_data_segura(x, formato_saida))
    return df

def _parse_data_segura(valor, formato_saida):
    if pd.isnull(valor):
        return np.nan
    try:
        valor_str = str(valor)
        dt = dateparser.parse(valor_str)
        if dt:
            return dt.strftime(formato_saida)
    except Exception:
        pass
    return np.nan

# Função para normalizar nomes das colunas: minúsculo e sem acentos e sem espaços
def normalizar_coluna(col):
    col = col.strip().lower()  # remove espaços e coloca em minúsculo
    col = unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('utf-8')  # remove acentos
    col = col.replace(' ', '_')  # opcional: troca espaços por underline
    return col


# Função para remover emojis e caracteres especiais, mantendo apenas letras, números, espaços e pontuação simples
def limpar_texto(texto):
    if isinstance(texto, str):
        # Remove qualquer caractere que não seja texto, número, espaço, vírgula, ponto, hífen ou underline
        return re.sub(r'[^\w\s,.\-]', '', texto)
    return texto  # mantém NaN ou outros como estão


# Função para converter decimal (vírgula ou ponto) em horas:minutos
def converter_para_horas_minutos(valor):
    try:
        if pd.isnull(valor):
            return np.nan
        
        # Troca vírgula por ponto se necessário
        if isinstance(valor, str):
            valor = valor.strip().replace(',', '.')

        # Converte para float
        horas_float = float(valor)
        if horas_float < 0:
            return np.nan

        horas = int(horas_float)
        minutos = int(round((horas_float - horas) * 60))
        
        return f'{horas:02d}:{minutos:02d}'
    
    except:
        return np.nan

###############################################################################################
# TRATAMENTO DOS DADOS UTILIZANDO AS FUNÇÕES CRIADAS
###############################################################################################

# Colunas de data a serem tratadas
colunas_data = [
    'target_date',
    'deadline',
    'planned_end_date',
    'planned_start_date',
    'start_date',
    'Data_inicio_entrega',
    'Data_Fim_Entrega',
    'end_date'
]

# Colunas de data e hora a serem tratadas
colunas_data_hora = [
    'created_at',
    'last_modified',
    'last_moved'
]

# Aplicar limpeza de emojis e caracteres não textuais nas colunas workflow e lane
colunas_str = ['workflow', 'lane']

# Substituir 'N/A' por 0 na coluna outcome_projection_value
df['outcome_projection_value'] = df['outcome_projection_value'].replace('N/A', 0)
df['outcome_projection_value'] = pd.to_numeric(df['outcome_projection_value'], errors='coerce').fillna(0)

# Ajustar as colunas de data com os devidos formatos
df = tratar_coluna_dataparser(df, colunas_data, formato_saida='%d/%m/%Y')
df = tratar_coluna_dataparser(df, colunas_data_hora, formato_saida='%d/%m/%Y %H:%M')

# Converter apenas valores que são strings, remover outros (substituir por NaN)
for col in colunas_str:
    if col in df.columns:
        df[col] = df[col].apply(lambda x: x if isinstance(x, str) else np.nan)

for col in colunas_str:
    if col in df.columns:
        df[col] = df[col].apply(limpar_texto)

# Lista das colunas de tempo originais para receberem tratamento
colunas_tempo = ['TempoDecorrido', 'cycle_time', 'block_time', 'logged_time']

# Criar novas colunas formatadas sem alterar as originais
for col in colunas_tempo:
    if col in df.columns:
        nova_coluna = f'{col}_hhmm'
        df[nova_coluna] = df[col].apply(converter_para_horas_minutos)

# Normalizar nome de colunas do DataFrame
df.columns = [normalizar_coluna(col) for col in df.columns]

###############################################################################################
# INSERINDO REGISTRO NO BANCO DE DADOS
###############################################################################################

# Configurações de conexão
usuario = 'root'
senha = 'ectt2025'
host = 'localhost'
porta = '5437'
banco = 'ecttdb'
tabela = 'projetos_softex_csv'

# Conecta ao PostgreSQL padrão (postgres) para criar banco
try:
    conn = psycopg2.connect(
        dbname='postgres',
        user=usuario,
        password=senha,
        host=host,
        port=porta
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    # Cria o banco se não existir
    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{banco}'")
    exists = cursor.fetchone()
    if not exists:
        cursor.execute(f'CREATE DATABASE {banco}')
        print(f"Banco '{banco}' criado com sucesso.")
    else:
        print(f"Banco '{banco}' já existe.")

    cursor.close()
    conn.close()
except Exception as e:
    print(f"Erro ao criar banco: {e}")
    exit()

# Conecta ao banco criado e envia o dataframe para a tabela
try:
    url_conexao = f'postgresql+psycopg2://{usuario}:{senha}@{host}:{porta}/{banco}'
    engine = create_engine(url_conexao)
    df.to_sql(tabela, engine, if_exists='replace', index=False)
    print(f"Tabela '{tabela}' criada no banco '{banco}' com sucesso!")
except Exception as e:
    print(f"Erro ao conectar ou enviar os dados: {e}")