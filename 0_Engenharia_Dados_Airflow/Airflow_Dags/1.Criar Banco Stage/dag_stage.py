from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta

# Define os argumentos padrão
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define a DAG
dag = DAG(
    'etl_dados_stage',
    default_args=default_args,
    description='DAG para executar o ETL dos dados para o estágio',
    schedule_interval='0 23 * * *',
)

# Tarefa para a etapa de conexão com a API e criação das tabelas
task_conexao_api = BashOperator(
    task_id='conexao_api_criacao_tabelas',
    bash_command='python3 /opt/airflow/dags/coleta_dados_api.py',
    dag=dag,
)

# Tarefa para a etapa de ETL (Extração, Transformação e Carregamento) dos dados
task_etl = BashOperator(
    task_id='etl_dados',
    bash_command='python3 /opt/airflow/dags/etl_dados_stage.py',
    dag=dag,
)

# Tarefa de envio de email em caso de sucesso na DAG - ETL de Dados
send_email_success_etl = EmailOperator(
    task_id='send_email_success_etl',
    to='nayara.valevskii@gmail.com',
    subject='Sucesso na execução da DAG ETL Dados Stage - ETL de Dados',
    html_content="""<h3>A execução da DAG ETL Dados Stage - ETL de Dados foi bem-sucedida.</h3>
                    <p>Os dados foram processados corretamente.</p>""",
    dag=dag,
    trigger_rule='all_success',  # Só envia o e-mail se todas as tarefas forem bem-sucedidas
)

# Define as dependências entre as tarefas
task_conexao_api >> task_etl >> send_email_success_etl
