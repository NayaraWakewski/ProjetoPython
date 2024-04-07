import pandas as pd
from sqlalchemy import create_engine, Numeric, DateTime, text
from dateutil.parser import parse

# Configuração do engine do SQLAlchemy para o PostgreSQL
engine = create_engine(
    'postgresql://airflow:airflow@host.docker.internal:5432/airflow',
    echo=False  # Desativado para não visualizar as instruções SQL geradas
)

# Definição das colunas de data e valor fora das funções para uso global
colunas_de_data = [
    'data_assinatura', 'data_processamento', 'data_termino',
    'data_publicacao_portal', 'data_publicacao_doe', 'data_auditoria',
    'data_termino_original', 'data_inicio', 'data_rescisao',
    'data_finalizacao_prestacao_contas'
]
colunas_de_valor = [
    'valor_contrato', 'valor_can_rstpg', 'valor_original_concedente',
    'valor_original_contrapartida', 'valor_atualizado_concedente',
    'valor_atualizado_contrapartida', 'calculated_valor_aditivo',
    'calculated_valor_ajuste', 'calculated_valor_empenhado',
    'calculated_valor_pago'
]

# Função para converter entrada em data de forma segura
def converter_para_datetime_seguro(coluna):
    convertido = []
    for entrada in coluna:
        try:
            data_convertida = pd.to_datetime(entrada)
        except (ValueError, TypeError):
            try:
                data_convertida = parse(entrada, default=pd.NaT)
            except (ValueError, TypeError):
                data_convertida = pd.NaT
        convertido.append(data_convertida)
    return pd.Series(convertido)

## Função para tratar os nulos e preencher os valores
def tratar_nulos(df):
    print(f"Antes do tratamento, contagem de nulos por coluna de data:")
    print(df[colunas_de_data].isnull().sum())
    
    # Tratamento para colunas de data
    for col in colunas_de_data:
        df[col] = converter_para_datetime_seguro(df[col])
    
    # Tratamento para colunas numéricas
    for col in colunas_de_valor:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.00)
    
    # Tratamento para colunas de texto
    outras_colunas = [col for col in df.columns if col not in colunas_de_data + colunas_de_valor]
    df[outras_colunas] = df[outras_colunas].fillna("Não Informado")
    
    # Tratamento para colunas booleanas
    colunas_booleanas = [col for col in df.columns if df[col].dtype == 'bool']
    df[colunas_booleanas] = df[colunas_booleanas].fillna(False)
    
    return df

# Função para normalizar colunas de texto para maiúsculas
def normalizar_colunas_texto(df):
    colunas_texto = [col for col in df.columns if col not in colunas_de_data + colunas_de_valor]
    for col in colunas_texto:
        if df[col].dtype == 'object':
            df[col] = df[col].str.upper()
    return df

# Função para remover duplicatas
def remover_duplicatas(df):
    numero_de_linhas_inicial = df.shape[0]
    print(f"Total de linhas antes da remoção de duplicatas: {numero_de_linhas_inicial}")
    
    # Removendo duplicatas com base nas colunas-chave
    df = df.drop_duplicates(subset=['cpf_cnpj_financiador', 'num_contrato', 'data_assinatura'])
    
    numero_de_linhas_final = df.shape[0]
    print(f"Total de linhas após a remoção de duplicatas: {numero_de_linhas_final}")
    print(f"Duplicatas removidas: {numero_de_linhas_inicial - numero_de_linhas_final}")
    
    return df

# Função para extrair dados do banco de dados
def extrair_dados(engine, esquema, nome_tabela):
    query = f"SELECT * FROM {esquema}.{nome_tabela};"
    with engine.connect() as conexao:
        df = pd.read_sql(query, conexao)
    return df

# Função para carregar dados no banco de dados com tipos definidos
def carregar_dados(engine, df, esquema, nome_tabela):
    mapeamento_de_tipos = {
        col: DateTime() for col in colunas_de_data
    }
    mapeamento_de_tipos.update({
        col: Numeric(20, 2) for col in colunas_de_valor
    })
    
    with engine.begin() as conexao:
        conexao.execute(text(f"DROP TABLE IF EXISTS {esquema}.{nome_tabela};"))
        df.to_sql(nome_tabela, conexao, schema=esquema, if_exists='replace', index=False, method='multi', dtype=mapeamento_de_tipos)
    print(f"Dados carregados na tabela '{esquema}.{nome_tabela}'")

# Função principal para realizar ETL para ambas as tabelas
def pipeline_etl(engine, esquema, nome_tabela):
    print(f"Iniciando ETL para {esquema}.{nome_tabela}")
    df = extrair_dados(engine, esquema, nome_tabela)
    df = tratar_nulos(df)
    df = normalizar_colunas_texto(df)
    df = remover_duplicatas(df)
    carregar_dados(engine, df, esquema, nome_tabela)
    
    # Adicionando a chave única após a carga de dados
    with engine.begin() as conexao:
        nome_constraint = f"{nome_tabela}_chave_unica"
        sql_adicionar_constraint = text(f"""
            ALTER TABLE {esquema}.{nome_tabela}
            DROP CONSTRAINT IF EXISTS {nome_constraint},
            ADD CONSTRAINT {nome_constraint} UNIQUE (cpf_cnpj_financiador, num_contrato, data_assinatura);
        """)
        conexao.execute(sql_adicionar_constraint)
    print(f"Chave única adicionada à tabela '{esquema}.{nome_tabela}'")

# Executando o ETL para as tabelas 'contratos' e 'convenios' no esquema 'stage'
for tabela in ['contratos', 'convenios']:
    pipeline_etl(engine, 'stage', tabela)
