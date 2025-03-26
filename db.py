import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import VARCHAR, FLOAT, BOOLEAN, TIMESTAMP, DATE, TIME, INTEGER, TEXT

# Configurações de conexão
usuario = 'root'
senha = 'ectt2025'
host = 'localhost'
porta = '5437'
banco = 'ecttdb'
tabela = 'projetos_softex'

# Conecta postgres, cria banco da aplicação 
def conecta_cria_db():
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

# Mapeamento de tipos de dados por campo
dtype_map = {
    'item_id': INTEGER,
    'item_name': VARCHAR(255),
    'item_type': VARCHAR(100),
    'outcome_projection_value': INTEGER,
    'work_progress_value': INTEGER,
    'work_progress_expected_value': INTEGER,
    'target_date': DATE,
    'board': VARCHAR(100),
    'workflow': VARCHAR(50),
    'work_item': VARCHAR(50),
    'column': VARCHAR(50),
    'lane': VARCHAR(100),
    'block_count': INTEGER,
    'block_reason': TEXT,
    'block_time': FLOAT,
    'is_blocked': BOOLEAN,
    'blocker_label': VARCHAR(100),
    'created_at': TIMESTAMP,
    'custom_card_id': VARCHAR(100),
    'cycle_time': FLOAT,
    'deadline': DATE,
    'end_date': DATE,
    'last_modified': TIMESTAMP,
    'last_moved': TIMESTAMP,
    'logged_time': FLOAT,
    'planned_end_date': DATE,
    'planned_start_date': DATE,
    'position_in_cell': INTEGER,
    'priority': VARCHAR(50),
    'section': VARCHAR(100),
    'size': INTEGER,
    'start_date': DATE,
    'stickers': VARCHAR(255),
    'tags': VARCHAR(255),
    'type': VARCHAR(100),
    'workspace': VARCHAR(100),
    'parent_id': VARCHAR(100),
    'natureza_do_item': VARCHAR(200),
    'tempodecorrido': FLOAT,
    'data_inicio_entrega': DATE,
    'data_fim_entrega': DATE,
    'quadro': VARCHAR(30),
    'secao': VARCHAR(30),
    'tempodecorrido_hhmm': TEXT,
    'cycle_time_hhmm': TEXT,
    'block_time_hhmm': TEXT,
    'logged_time_hhmm': TEXT
}

# Conecta ao banco criado e envia o dataframe para a tabela
def envia_df_to_table(df):
    try:
        url_conexao = f'postgresql+psycopg2://{usuario}:{senha}@{host}:{porta}/{banco}'
        engine = create_engine(url_conexao)
        
        df.to_sql(
            tabela,
            engine,
            if_exists='replace',
            index=False,
            dtype=dtype_map
        )
        print(f"Tabela '{tabela}' criada no banco '{banco}'.")
    except Exception as e:
        print(f"Erro ao conectar ou enviar os dados: {e}")