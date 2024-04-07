from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
dag = DAG(
    'limpeza_dados',
    default_args=default_args,
    description='DAG para executar a limpeza dos dados',
    schedule_interval=None, # Sem agendamento automático
    catchup=False, # Evita execuções retroativas
)

# Tarefa para a execução do script Python
task_executar_limpeza = BashOperator(
    task_id='executar_script_limpeza',
    bash_command='python /caminho/para/seu/script/testelimpeza.py ',
    dag=dag,
)

task_executar_limpeza
