import pandas as pd
import numpy as np
import unicodedata, re, dateparser, psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import VARCHAR, FLOAT, BOOLEAN, TIMESTAMP, DATE, TIME, INTEGER, TEXT

###############################################################################################
# DEFINIÇÃO DE FUNÇÕES PARA TRATAMENTO DE DADOS
###############################################################################################

# Função genérica para conversão de datas
def _parse_data_segura(valor):
    if pd.isnull(valor):
        return np.nan
    try:
        valor_str = str(valor)
        dt = dateparser.parse(valor_str)
        return dt if dt else np.nan
    except Exception:
        return np.nan

def tratar_coluna_data(df, colunas):
    for col in colunas:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def tratar_coluna_dataparser(df, colunas):
    for col in colunas:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: _parse_data_segura(x))
    return df


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

# Converte valores de uma coluna para boole
def para_booleano(valor):
    if isinstance(valor, str):
        valor = valor.strip().lower()
        return valor in ['yes', 'sim', 'true', '1']
    return bool(valor)

# Função para tratar colunas numéricas com vírgula, trocando para ponto
def tratar_colunas_numericas(df, colunas):
    for col in colunas:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(',', '.', regex=False)
                .str.replace(' ', '')
                .replace('', np.nan)
            )
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

# Função para trocar valores N/A por zero
def substituir_na_por_zero(df, colunas):
    for coluna in colunas:
        df[coluna] = df[coluna].replace('N/A', 0)
        df[coluna] = pd.to_numeric(df[coluna], errors='coerce').fillna(0)
    return df