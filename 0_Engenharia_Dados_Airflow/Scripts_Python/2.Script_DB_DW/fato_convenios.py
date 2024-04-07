import pandas as pd
from sqlalchemy import create_engine, text
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuração do engine do SQLAlchemy para conectar ao PostgreSQL
engine = create_engine('postgresql://airflow:airflow@host.docker.internal:5432/airflow', echo=False)

def criar_schema_dw():
    with engine.begin() as conn:
        try:
            conn.execute(text("CREATE SCHEMA dw;"))
            logging.info("Schema DW criado.")
        except Exception as e:
            logging.info("Schema DW já existe.")

def criar_tabela_fato_convenios():
    comando_sql = """
        CREATE TABLE IF NOT EXISTS dw.fato_convenios (
            id_fato_convenio SERIAL PRIMARY KEY,
            valor_contrato NUMERIC,
            valor_can_rstpg NUMERIC,
            valor_original_concedente NUMERIC,
            valor_original_contrapartida NUMERIC,
            valor_atualizado_concedente NUMERIC,
            valor_atualizado_contrapartida NUMERIC,
            calculated_valor_aditivo NUMERIC,
            calculated_valor_ajuste NUMERIC,
            calculated_valor_empenhado NUMERIC,
            calculated_valor_pago NUMERIC,
            data_assinatura DATE,
            data_processamento DATE,
            data_termino DATE,
            data_publicacao_doe DATE,
            data_auditoria DATE,
            data_termino_original DATE,
            data_inicio DATE,
            data_rescisao DATE,
            data_finalizacao_prestacao_contas DATE,
            id_entidade BIGINT REFERENCES dw.dim_entidade(id_entidade),
            id_modalidade BIGINT REFERENCES dw.dim_modalidade(id_modalidade),
            id_projeto BIGINT REFERENCES dw.dim_projeto(id_projeto),
            id_contrato BIGINT REFERENCES dw.dim_contrato(id_contrato),
            id_participacao BIGINT REFERENCES dw.dim_participacao(id_participacao)
        );
    """

    with engine.begin() as conn:
        conn.execute(text(comando_sql))
        logging.info("Tabela fato_convenios criada ou já existente.")

def extrair_dados(engine, nome_tabela, schema='stage'):
    query = f"SELECT * FROM {schema}.{nome_tabela};"
    df = pd.read_sql_query(query, engine)
    logging.info(f"Dados extraídos de {schema}.{nome_tabela} com sucesso.")
    return df

def carregar_dados_fato_convenios(engine, df):
    schema = 'dw'
    nome_tabela = 'fato_convenios'
    df.to_sql(nome_tabela, engine, schema=schema, if_exists='append', index=None, method='multi')
    logging.info(f"Dados carregados em {schema}.{nome_tabela} com sucesso.")
    
def limpar_tabelas():
    tabelas = [
        'dw.fato_convenios',
        # Adicione os nomes de outras tabelas aqui
    ]
    with engine.begin() as conn:
        for tabela in tabelas:
            try:
                conn.execute(text(f"TRUNCATE TABLE {tabela} RESTART IDENTITY CASCADE;"))
                logging.info(f"Tabela {tabela} limpa com sucesso.")
            except Exception as e:
                logging.warning(f"Tabela {tabela} não pôde ser limpa: {str(e)}")
            
def main():
    # Criar o schema DW se ele ainda não existir
    criar_schema_dw()
    
    # Criar a tabela fato_contratos no schema DW
    criar_tabela_fato_convenios()
    
    # Limpar as tabelas antes de inserir novos dados
    limpar_tabelas()
    
    # Criar a tabela fato_contratos no schema DW
    criar_tabela_fato_convenios()
      
    # Extrair os dados das tabelas 'convenios' e 'contratos' no schema 'stage'
    df_convenios = extrair_dados(engine, 'convenios')
    
    # # Verifique se a coluna está presente e renomeie se necessário
    # if 'descriaco_edital' in df_convenios.columns:
    #     df_convenios.rename(columns={'descriaco_edital': 'descricao_edital'}, inplace=True)
    # if 'descriaco_edital' in df_contratos.columns:
    #     df_contratos.rename(columns={'descriaco_edital': 'descricao_edital'}, inplace=True)

    # Concatenar os DataFrames para formar o DataFrame final para a tabela dim_projeto
    df_fato_convenios = df_convenios[[
    'valor_contrato',
    'valor_can_rstpg',
    'valor_original_concedente',
    'valor_original_contrapartida',
    'valor_atualizado_concedente',
    'valor_atualizado_contrapartida',
    'calculated_valor_aditivo',
    'calculated_valor_ajuste',
    'calculated_valor_empenhado',
    'calculated_valor_pago',
    'data_assinatura',
    'data_processamento',
    'data_termino',
    'data_publicacao_doe',
    'data_auditoria',
    'data_termino_original',
    'data_inicio',
    'data_rescisao',
    'data_finalizacao_prestacao_contas'
]]
    
    # Carregar os dados na tabela dim_projeto
    carregar_dados_fato_convenios(engine, df_fato_convenios)
    
    logging.info("Processo de ETL concluído com sucesso.")

if __name__ == "__main__":
    main()
