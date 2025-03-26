import pandas as pd
import numpy as np
from softex_funcs import tratar_colunas_numericas, tratar_coluna_dataparser, normalizar_coluna, limpar_texto, converter_para_horas_minutos, para_booleano, substituir_na_por_zero
from db import conecta_cria_db, envia_df_to_table


# Carregar dados do Google Sheets
# https://docs.google.com/spreadsheets/d/1sH2xZoKWklZWirYo1K9EEg167CQV55bbh-ycH3poZPQ/export?format=csv&id=1sH2xZoKWklZWirYo1K9EEg167CQV55bbh-ycH3poZPQ&gid=0
sheet_id = "1sH2xZoKWklZWirYo1K9EEg167CQV55bbh-ycH3poZPQ"
gid = 0
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
#df = pd.read_excel(url)
df = pd.read_csv(url)
df = df.dropna(how='all')

###############################################################################################
# Realizando tratamento de dados
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

# Ajustar as colunas de data com os devidos formatos
df = tratar_coluna_dataparser(df, colunas_data)
df = tratar_coluna_dataparser(df, colunas_data_hora)

# Colunas numéricas para serem tratadas
colunas_float = [
    'outcome_projection_value',
    'work_progress_value',
    'work_progress_expected_value',
    'block_time',
    'logged_time',
    'cycle_time',
    'TempoDecorrido'
]

# Tratar as colunas numéricas
df = tratar_colunas_numericas(df, colunas_float)

# Colunas com N/A para converter em zero
colunas_tratar_na_zero = [
    'outcome_projection_value'
]
df = substituir_na_por_zero(df, colunas_tratar_na_zero)

# Aplicar limpeza de emojis e caracteres não textuais nas colunas workflow e lane
colunas_str = [
    'workflow', 
    'lane'
]

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

# Converter os dado do campo is_blocked para booleano
df['is_blocked'] = df['is_blocked'].apply(para_booleano)

# Normalizar nome de colunas do DataFrame
df.columns = [normalizar_coluna(col) for col in df.columns]

# print(df.columns)
# print(df)


###############################################################################################
# Inserindo os registros no PostgreSQLS
###############################################################################################

conecta_cria_db()
envia_df_to_table(df)