import pandas as pd
from sqlalchemy import create_engine, text
import logging

# Configuração de registro de eventos para rastrear e relatar a execução do script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuração do engine do SQLAlchemy para conectar ao banco de dados PostgreSQL
engine = create_engine('postgresql://airflow:airflow@host.docker.internal:5432/airflow', echo=False)

# Função para criar o schema 'dw' no banco de dados se ele ainda não existir
def criar_schema_dw():
    with engine.begin() as conn:
        try:
            # Cria o schema 'dw'
            conn.execute(text("CREATE SCHEMA dw;"))
            logging.info("Schema DW criado.")
        except Exception as e:
            # Se o schema 'dw' já existir, um log é registrado
            logging.info("Schema DW já existe.")

# Função para criar a tabela 'dim_participacao' dentro do schema 'dw'
def criar_tabela_dim_participacao():
    comando_sql = """
        CREATE TABLE IF NOT EXISTS dw.dim_participacao (
            id_participacao SERIAL PRIMARY KEY, --- Chave primária com auto incremento
            isn_parte_destino BIGINT,           --- Coluna para parte destinatária
            isn_parte_origem TEXT,              --- Coluna para parte de origem
            isn_sic BIGINT,                     --- Coluna para identificador do SIC
            isn_entidade BIGINT,                --- Coluna para identificador da entidade
            gestor_contrato TEXT,               --- Coluna para o gestor do contrato
            num_certidao TEXT                   --- Coluna para número da certidão
        );
    """
    # Executa o comando SQL para criar a tabela
    with engine.begin() as conn:
        conn.execute(text(comando_sql))
        logging.info("Tabela dim_participacao criada ou já existente.")

# Função para extrair dados de uma tabela específica no schema 'stage'
def extrair_dados(engine, nome_tabela, schema='stage'):
    query = f"SELECT * FROM {schema}.{nome_tabela};"
    # Executa a query e retorna um DataFrame com os resultados
    df = pd.read_sql_query(query, engine)
    logging.info(f"Dados extraídos de {schema}.{nome_tabela} com sucesso.")
    return df

# Função para carregar dados no banco de dados dentro da tabela 'dim_participacao'
def carregar_dados_dim_participacao(engine, df):
    schema = 'dw'
    nome_tabela = 'dim_participacao'
    # Insere os dados no banco de dados
    df.to_sql(nome_tabela, engine, schema=schema, if_exists='append', index=None, method='multi')
    logging.info(f"Dados carregados em {schema}.{nome_tabela} com sucesso.")
    
# Função para limpar tabelas antes de carregar novos dados para evitar duplicações
def limpar_tabelas():
    tabelas = [
        'dw.dim_participacao',  # Lista das tabelas a serem limpas
        # Adicione os nomes de outras tabelas aqui
    ]
    with engine.begin() as conn:
        for tabela in tabelas:
            try:
                # Limpa a tabela, reiniciando os IDs e removendo dependências
                conn.execute(text(f"TRUNCATE TABLE {tabela} RESTART IDENTITY CASCADE;"))
                logging.info(f"Tabela {tabela} limpa com sucesso.")
            except Exception as e:
                logging.warning(f"Tabela {tabela} não pôde ser limpa: {str(e)}")
            
# Função principal que executa as etapas do processo ETL
def main():
    # Criar o schema DW se ele ainda não existir
    criar_schema_dw()
    
    # Criar a tabela dim_participacao no schema DW
    criar_tabela_dim_participacao()
    
    # Limpar as tabelas antes de inserir novos dados
    limpar_tabelas()
    
    # Criar a tabela dim_participacao no schema DW
    criar_tabela_dim_participacao()
    
    # Extrair os dados das tabelas 'convenios' e 'contratos' no schema 'stage'
    df_convenios = extrair_dados(engine, 'convenios')
    df_contratos = extrair_dados(engine, 'contratos')
    
    # Concatenar os DataFrames para formar o DataFrame final para a tabela dim_participacao
    df_dim_participacao = pd.concat([df_convenios[['isn_parte_destino', 'isn_parte_origem', 'isn_sic', 'isn_entidade', 'gestor_contrato', 'num_certidao']],
                                     df_contratos[['isn_parte_destino', 'isn_parte_origem', 'isn_sic', 'isn_entidade', 'gestor_contrato', 'num_certidao']]])
    
    # Carregar os dados na tabela dim_participacao
    carregar_dados_dim_participacao(engine, df_dim_participacao)
    
    logging.info("Processo de ETL concluído com sucesso.")

# Ponto de entrada do script
if __name__ == "__main__":
    main()
