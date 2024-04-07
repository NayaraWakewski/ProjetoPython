from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_dimensoes_fatos',
    default_args=default_args,
    description='DAG para executar ETL das dimensões e fatos',
    schedule_interval=None,  # A DAG não é agendada, mas sim acionada
    is_paused_upon_creation=False,  # Não pausa a DAG na criação, permitindo que ela seja acionada externamente
)

task_criar_dimensoes = BashOperator(
    task_id='criar_dimensoes',
    bash_command=(
        'python3 /opt/airflow/dags/dim_entidade.py && '
        'python3 /opt/airflow/dags/dim_modalidade.py && '
        'python3 /opt/airflow/dags/dim_projeto.py && '
        'python3 /opt/airflow/dags/dim_contrato.py && '
        'python3 /opt/airflow/dags/dim_participacao.py'
    ),
    dag=dag,
)

task_criar_fatos = BashOperator(
    task_id='criar_fatos',
    bash_command=(
        'python3 /opt/airflow/dags/fato_contratos.py && '
        'python3 /opt/airflow/dags/fato_convenios.py'
    ),
    dag=dag,
)

send_email_success = EmailOperator(
    task_id='send_email_success',
    to='nayara.valevskii@gmail.com',
    subject='Sucesso na execução da DAG de criação das dimensões e fatos',
    html_content='<p>Todas as dimensões e fatos foram criados com sucesso!</p>',
    dag=dag
)

# Defina a sequência de execução das tarefas
task_criar_dimensoes >> task_criar_fatos >> send_email_success
