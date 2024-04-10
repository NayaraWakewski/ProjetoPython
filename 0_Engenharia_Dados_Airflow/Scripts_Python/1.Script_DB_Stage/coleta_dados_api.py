import requests
import pandas as pd
from sqlalchemy import create_engine, inspect, text
import logging
from sqlalchemy.exc import SQLAlchemyError

# Configuração do logging para registrar mensagens de informação
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# URLs das APIs de onde os dados serão obtidos
url_contratos = 'https://api-dados-abertos.cearatransparente.ce.gov.br/transparencia/contratos/contratos'
url_convenios = 'https://api-dados-abertos.cearatransparente.ce.gov.br/transparencia/contratos/convenios'

# Configuração do engine do SQLAlchemy para o PostgreSQL
engine = create_engine('postgresql://airflow:airflow@host.docker.internal:5432/airflow', echo=False)

# Função para obter dados de uma URL
def obter_dados(url, num_paginas):
    # Inicializa uma lista vazia para armazenar todos os dados obtidos
    todos_os_dados = []
    
    # Loop sobre o número de páginas especificado
    for pagina in range(1, num_paginas + 1):
        parametros = {'page': pagina}
        
        # Faz uma requisição GET para a URL especificada com os parâmetros da página
        resposta = requests.get(url, params=parametros, headers={'accept': 'application/json'}, timeout=30)
        
        # Verifica se a resposta da requisição foi bem-sucedida (status code 200)
        if resposta.status_code == 200:
            # Extrai os dados da resposta no formato JSON
            dados = resposta.json()
            
            # Verifica se não há mais dados a serem obtidos, e encerra o loop nesse caso
            if not dados['data']:
                break
            
            # Adiciona os dados obtidos à lista de todos os dados
            todos_os_dados.extend(dados['data'])
            logging.info(f'Página {pagina}/{num_paginas} processada.')
        else:
            # Registra um erro se a requisição não foi bem-sucedida e retorna um DataFrame vazio
            logging.error(f'Erro na requisição: {resposta.status_code}')
            return pd.DataFrame()
    
    # Converte a lista de todos os dados em um DataFrame e retorna
    return pd.DataFrame(todos_os_dados)

# Função para criar um schema no banco de dados se ele não existir
def criar_schema_se_nao_existir(engine, schema):
    try:
        # Conecta-se ao banco de dados usando o engine especificado
        with engine.connect() as conn:
            # Cria uma query SQL para criar o schema se ele não existir
            query = f"CREATE SCHEMA IF NOT EXISTS {schema};"
            
            # Executa a query no banco de dados
            conn.execute(query)
            
            # Registra uma mensagem de informação se o schema foi criado com sucesso
            logging.info(f'Schema {schema} criado com sucesso.')
    except SQLAlchemyError as e:
        # Registra um erro se ocorrer algum problema ao criar o schema
        logging.error(f'Erro ao verificar ou criar o schema {schema}: {e}')

# Função para criar uma tabela no banco de dados se ela não existir
def criar_tabela_se_nao_existir(engine, nome_tabela, schema, df):
    # Cria um inspetor para verificar a existência da tabela
    inspector = inspect(engine)
    
    # Verifica se a tabela não existe
    if not inspector.has_table(nome_tabela, schema=schema):
        # Registra uma mensagem de informação se a tabela não existe e será criada
        logging.info(f'A tabela {nome_tabela} não existe. Criando...')
        
        # Cria uma tabela vazia com a estrutura do DataFrame fornecido
        df.head(0).to_sql(nome_tabela, engine, schema=schema, index=False)
    else:
        # Registra uma mensagem de informação se a tabela já existe
        logging.info(f'A tabela {nome_tabela} já existe.')

# Função para verificar se existem dados duplicados em uma tabela
def verificar_dados_existentes(engine, nome_tabela, schema, df, colunas_chave_unica):
    try:
        # Conecta-se ao banco de dados usando o engine especificado
        with engine.connect() as conn:
            # Constrói uma consulta SQL para verificar a existência de dados duplicados
            condicoes = " AND ".join([f"{col}='{df[col].iloc[0]}'" for col in colunas_chave_unica])
            sql = f"SELECT COUNT(*) FROM {schema}.{nome_tabela} WHERE {condicoes};"
            
            # Executa a consulta SQL
            result = conn.execute(text(sql))
            
            # Obtém o número de linhas retornadas pela consulta
            row_count = result.fetchone()[0]
            
            # Retorna True se os dados existirem, False caso contrário
            return row_count > 0
    except SQLAlchemyError as e:
        # Registra um erro se ocorrer algum problema ao verificar os dados duplicados
        logging.error(f'Erro ao verificar dados existentes na tabela {schema}.{nome_tabela}: {e}')
        return False

# Função para criar uma tabela e inserir dados nela usando uma tabela temporária
def criar_tabela_e_inserir_dados_com_tabela_temporaria(df, nome_tabela, engine, colunas_chave_unica, schema='stage'):
    # Verifica se o DataFrame está vazio
    if df.empty:
        # Registra uma mensagem de informação se o DataFrame estiver vazio
        logging.info(f'Nenhum dado disponível para a tabela {nome_tabela}.')
        return

    # Define os nomes da tabela temporária e da tabela definitiva
    nome_tabela_temporaria = f"{nome_tabela}_temp_{schema}"
    nome_tabela_definitiva = f"{nome_tabela}"

    try:
        # Insere os dados na tabela temporária apenas se eles não existirem na tabela definitiva
        if not verificar_dados_existentes(engine, nome_tabela_definitiva, schema, df, colunas_chave_unica):
            # Insere os dados na tabela temporária
            df.to_sql(nome_tabela_temporaria, engine, if_exists='replace', index=False, schema=schema)
            logging.info(f'Dados inseridos na tabela temporária {schema}.{nome_tabela_temporaria}.')

            # Tenta inserir os dados na tabela definitiva
            with engine.begin() as conn:
                df.to_sql(nome_tabela_definitiva, engine, if_exists='append', index=False, schema=schema, method='multi')
                logging.info(f'Dados inseridos na tabela {schema}.{nome_tabela_definitiva} com sucesso.')

            # Remove a tabela temporária após a inserção
            with engine.begin() as conn:
                query = f'DROP TABLE IF EXISTS "{schema}"."{nome_tabela_temporaria}";'
                conn.execute(text(query))
                logging.info(f'Tabela temporária {schema}.{nome_tabela_temporaria} removida com sucesso.')
        else:
            # Registra uma mensagem de informação se os dados já existirem na tabela definitiva
            logging.info(f'Dados já existentes na tabela {schema}.{nome_tabela_definitiva}. Nenhuma inserção foi realizada.')
    except SQLAlchemyError as e:
        # Registra um erro se ocorrer algum problema ao inserir dados na tabela definitiva
        logging.error(f'Erro ao inserir dados na tabela {schema}.{nome_tabela_definitiva}: {e}')


if __name__ == "__main__":
    # Cria o schema 'stage' se ele não existir
    criar_schema_se_nao_existir(engine, 'stage')
    
    # Obtém os dados dos contratos e dos convênios
    df_contratos = obter_dados(url_contratos, 10)
    df_convenios = obter_dados(url_convenios, 10)
    
    # Define as colunas de chave única para os contratos e convênios
    colunas_chave_unica_contratos = ['cpf_cnpj_financiador', 'num_contrato', 'data_assinatura']
    colunas_chave_unica_convenios = ['cpf_cnpj_financiador', 'num_contrato', 'data_assinatura']

    # Cria as tabelas 'contratos' e 'convenios' se elas não existirem
    criar_tabela_se_nao_existir(engine, 'contratos', 'stage', df_contratos)
    criar_tabela_se_nao_existir(engine, 'convenios', 'stage', df_convenios)

    # Cria e insere dados nas tabelas 'contratos' e 'convenios' usando tabelas temporárias
    criar_tabela_e_inserir_dados_com_tabela_temporaria(df_contratos, 'contratos', engine, colunas_chave_unica_contratos)
    criar_tabela_e_inserir_dados_com_tabela_temporaria(df_convenios, 'convenios', engine, colunas_chave_unica_convenios)
