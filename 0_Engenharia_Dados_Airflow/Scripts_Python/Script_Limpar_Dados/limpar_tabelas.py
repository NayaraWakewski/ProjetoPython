import psycopg2
from psycopg2 import sql

# Parâmetros de conexão com o banco de dados
params = {
    'host': '127.0.0.1',
    'port': '5432',
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow'
}

# Comandos SQL para exclusão de tabelas
sql_commands = {
    'grant': "GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;",
    'drop_fatos': [
        "DROP TABLE IF EXISTS dw.fato_contratos CASCADE;",
        "DROP TABLE IF EXISTS dw.fato_convenios CASCADE;"
    ],
    'drop_dimensoes': [
        "DROP TABLE IF EXISTS dw.dim_modalidade CASCADE;",
        "DROP TABLE IF EXISTS dw.dim_projeto CASCADE;",
        "DROP TABLE IF EXISTS dw.dim_entidade CASCADE;",
        "DROP TABLE IF EXISTS dw.dim_contrato CASCADE;",
        "DROP TABLE IF EXISTS dw.dim_participacao CASCADE;"
    ],
    'drop_stage': [
        "DROP TABLE IF EXISTS stage.contratos CASCADE;",
        "DROP TABLE IF EXISTS stage.convenios CASCADE;"
    ]
}

def execute_sql_commands(cursor, commands):
    for command in commands:
        cursor.execute(command)

def main():
    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()

        # Conceder permissões
        cursor.execute(sql_commands['grant'])
        print("Permissões concedidas com sucesso.")

        # Excluir tabelas de fato, dimensões e stage em sequência
        for key in ['drop_fatos', 'drop_dimensoes', 'drop_stage']:
            execute_sql_commands(cursor, sql_commands[key])
            print(f"Tabelas {key.replace('drop_', '').title()} excluídas com sucesso.")

        # Confirmar as alterações
        conn.commit()

    except psycopg2.Error as e:
        print("Erro ao conectar ou executar comandos SQL:", e)
        conn.rollback()

    finally:
        # Fechar o cursor e a conexão
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
