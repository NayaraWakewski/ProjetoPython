#ESSE SCRIPT SERVE PARA LIMPAR AS TABELAS COMPLETAMENTE, FACILITA NOS TESTES.
#APESAR DE NOS SCRIPTS TER UMA OPÇÃO DE LIMPAR, NO CASO DAS DIMENSÕES E FATO QUE TEM PRIMARY KEY E  FOREIGN KEY, 
#NÃO ACHEI MUITO SEGURO COLOCAR NA FUNÇÃO CASCADE, OU SEJA EXCLUIR A DE TODA FORMA, IDEAL É APENAS LIMPAR SEMPRE,
#PARA QUE OS DADOS NÃO SEJAM INSERIDOS DUPLICADOS.
#NESSE SCRIPT VOCÊ RODA ELE NA SEQUENCIA E COMENTANDO O QUE NÃO FOR USAR:
#EXEMPLO, SE EU QUERO EXCLUIR AS DIMENSOES POR ALGUM ERRO, EXCLUA AS FATOS PRIMEIRO, DEPOIS AS DIMENSOES OU UMA UNICA DIMENSAO;
#FAÇA A ALTERAÇÃO E DEPOIS CRIE A DIMENSÃO OU AS DIMENSÕES E DEPOIS AS FATOS... JUSTAMENTE PARA NÃO DAR ERRO DE RELACIONAMENTO.

import psycopg2

# Parâmetros de conexão com o banco de dados
host = 'host.docker.internal'
port = '5432'
database = 'airflow'
user = 'airflow'
password = 'airflow'

# Comandos SQL para conceder permissões e excluir tabelas
grant_sql = "GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;"
drop_contratos_sql = "DROP TABLE IF EXISTS stage.contratos CASCADE;"
drop_convenios_sql = "DROP TABLE IF EXISTS stage.convenios CASCADE;"
#drop_dimensoes_sql = "DROP TABLE IF EXISTS dw.dim_modalidade CASCADE; DROP TABLE IF EXISTS dw.dim_projeto CASCADE; DROP TABLE IF EXISTS dw.dim_entidade CASCADE; DROP TABLE IF EXISTS dw.dim_contrato CASCADE; DROP TABLE IF EXISTS dw.dim_participacao CASCADE;"
#drop_fatos_sql = "DROP TABLE IF EXISTS dw.fato_contratos CASCADE; DROP TABLE IF EXISTS dw.fato_convenios CASCADE;"

try:
    # Conectar ao banco de dados
    conn = psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port
    )

    # Criar um cursor para executar comandos SQL
    cursor = conn.cursor()

    # Conceder permissões
    cursor.execute(grant_sql)
    print("Permissões concedidas com sucesso.")

    # # Excluir a tabela contratos
    cursor.execute(drop_contratos_sql)
    print("Tabela 'contratos' excluída com sucesso.")

    # # Excluir a tabela convenios
    cursor.execute(drop_convenios_sql)
    print("Tabela 'convenios' excluída com sucesso.")

    # Excluir as tabelas de dimensões
    # cursor.execute(drop_dimensoes_sql)
    # print("Tabelas de dimensões excluídas com sucesso.")

    # Excluir as tabelas de fatos
    # cursor.execute(drop_fatos_sql)
    # print("Tabelas de fatos excluídas com sucesso.")

    # Confirmar as alterações
    conn.commit()

except psycopg2.Error as e:
    print("Erro ao conectar ou executar comandos SQL:", e)

finally:
    # Fechar o cursor e a conexão
    if cursor:
        cursor.close()
    if conn:
        conn.close()
