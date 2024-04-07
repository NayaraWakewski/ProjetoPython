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

def criar_tabela_dim_contrato():
    comando_sql = """
        CREATE TABLE IF NOT EXISTS dw.dim_contrato (
            id_contrato SERIAL PRIMARY KEY,
            num_contrato TEXT,
            plain_num_contrato TEXT,
            contract_type TEXT,
            infringement_status BIGINT,
            cod_financiador_including_zeroes TEXT,
            accountability_status TEXT,
            descricao_situacao TEXT
            );
    """

    with engine.begin() as conn:
        conn.execute(text(comando_sql))
        logging.info("Tabela dim_contrato criada ou já existente.")

def extrair_dados(engine, nome_tabela, schema='stage'):
    query = f"SELECT * FROM {schema}.{nome_tabela};"
    df = pd.read_sql_query(query, engine)
    logging.info(f"Dados extraídos de {schema}.{nome_tabela} com sucesso.")
    return df

def carregar_dados_dim_contrato(engine, df):
    schema = 'dw'
    nome_tabela = 'dim_contrato'
    df.to_sql(nome_tabela, engine, schema=schema, if_exists='append', index=None, method='multi')
    logging.info(f"Dados carregados em {schema}.{nome_tabela} com sucesso.")
    
def limpar_tabelas():
    tabelas = [
        'dw.dim_contrato',
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
    
    # Criar a tabela dim_projeto no schema DW
    criar_tabela_dim_contrato()
    
    # Limpar as tabelas antes de inserir novos dados
    limpar_tabelas()
    
    # Criar a tabela dim_projeto no schema DW
    criar_tabela_dim_contrato()
    
    # Criar a tabela dim_projeto no schema DW
    criar_tabela_dim_contrato()    
      
    # Extrair os dados das tabelas 'convenios' e 'contratos' no schema 'stage'
    df_convenios = extrair_dados(engine, 'convenios')
    df_contratos = extrair_dados(engine, 'contratos')
    
    # # Verifique se a coluna está presente e renomeie se necessário
    # if 'descriaco_edital' in df_convenios.columns:
    #     df_convenios.rename(columns={'descriaco_edital': 'descricao_edital'}, inplace=True)
    # if 'descriaco_edital' in df_contratos.columns:
    #     df_contratos.rename(columns={'descriaco_edital': 'descricao_edital'}, inplace=True)

    # Concatenar os DataFrames para formar o DataFrame final para a tabela dim_projeto
    df_dim_contrato = pd.concat([df_convenios[['num_contrato', 'plain_num_contrato', 'contract_type', 'infringement_status', 'cod_financiador_including_zeroes', 'accountability_status', 'descricao_situacao']],
                                   df_contratos[['num_contrato', 'plain_num_contrato', 'contract_type', 'infringement_status', 'cod_financiador_including_zeroes', 'accountability_status','descricao_situacao']]])
    
    # Carregar os dados na tabela dim_projeto
    carregar_dados_dim_contrato(engine, df_dim_contrato)
    
    logging.info("Processo de ETL concluído com sucesso.")

if __name__ == "__main__":
    main()
