from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
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
    'enviar_links_analises',
    default_args=default_args,
    description='DAG para enviar links das análises do PowerBI e documentação do GitHub',
    schedule_interval='@once',
)

# Tarefa Dummy, apenas para manter a estrutura. Você pode optar por removê-la se desejar.
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Tarefa de envio de email com os links
send_email_with_links = EmailOperator(
    task_id='send_email_with_links',
    to='nayara.valevskii@gmail.com',
    subject='Projeto de Análise de Informações de Convênios e Contratos do Governo do Estado do Ceará – Projeto das Alunas Nayara Valevskii e Isadora Xavier - {{ ds_nodash }}',
    html_content="""
        <html>
            <body>
                <h2 style="color: #2E86C1; font-family: Arial, sans-serif;">Projeto de Automação com Airflow e Docker e Análise de Dados com Python</h2>
                <p style="font-family: Arial, sans-serif;">Prezados,</p>
                <p style="font-family: Arial, sans-serif;">Com grande satisfação, compartilhamos com você os resultados do nosso projeto, uma iniciativa que explora a transparência de dados do Ceará Transparente através de processos automatizados e análises profundas feitas em Python.</p>
                <h3 style="color: #2E86C1; font-family: Arial, sans-serif;">Sobre o Projeto</h3>
                <p style="font-family: Arial, sans-serif;">Este projeto foi desenvolvido com o objetivo de extrair, analisar e reportar dados de contratos e convênios de maneira eficiente. Utilizamos Airflow, Docker e Python para automatizar a coleta de dados e armazená-los em um banco de dados STAGE no PostgreSQL, seguido de uma análise exploratória detalhada para identificar inconsistências; e a criação
                de um Data Warehouse no Postgres (DW) já com todos os tratamento efetuados, pronto para análises posteriores</p>
                <h3 style="color: #2E86C1; font-family: Arial, sans-serif;">Acesse as Análises e Documentação</h3>
                <p style="font-family: Arial, sans-serif;">Para uma visão detalhada do projeto, incluindo as análises realizadas e insights, acesse os links abaixo:</p>
                <ul style="font-family: Arial, sans-serif;">
                    <li><a href="https://app.powerbi.com/view?r=eyJrIjoiYWExZTliNzAtMDk0My00NTViLWI0ZDAtMmM0MTRhM2NkM2RkIiwidCI6ImJkMWMxZTAzLTU2MDMtNDUzNy04ODY5LWQ5ZGQyYzRiMjc2MiJ9&pageName=ReportSection" style="color: #2E86C1;">Análises no PowerBI</a></li>
                    <li><a href="https://github.com/NayaraWakewski/ProjetoPython" style="color: #2E86C1;">Documentação do Projeto no GitHub</a></li>
                </ul>
                <p style="font-family: Arial, sans-serif;">Através deste projeto, aprimoramos não apenas nossas habilidades técnicas e analíticas, mas também desenvolvemos competências essenciais como solução de problemas, pensamento crítico e comunicação eficaz.</p>
                <p style="font-family: Arial, sans-serif;">Atenciosamente,</p>
                <p style="font-family: Arial, sans-serif;"><b>Nayara Valevskii</b> (<a href="https://www.linkedin.com/in/nayaraba/" style="color: #2E86C1;">LinkedIn</a>) e <b>Isadora Xavier</b> (<a href="https://www.linkedin.com/in/isadora-xavier/" style="color: #2E86C1;">LinkedIn</a>)</p>
            </body>
        </html>
    """,
    dag=dag,
)

# Define as dependências entre as tarefas
start_task >> send_email_with_links
