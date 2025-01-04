from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import numpy as np

def numero_1 (ti):
    n = np.random.randint(1,10)
    ti.xcom_push(key='numero',value=n)
    return 0

def numero_2 ():
    n = np.random.randint(1,10)
    return n


with DAG (
    dag_id='Aprendendo_xcoms',
    schedule_interval='@daily',
    start_date=datetime(2024,12,31),
    catchup=True
) as dag:
    
    inicio = DummyOperator(task_id='Inicio')

    python_1 = PythonOperator(task_id='Python_1',python_callable=numero_1)

    python_2 = PythonOperator(task_id='Python_2',python_callable=numero_2)
    
    teste_bash = BashOperator(task_id='bash_1',
                              bash_command='echo "Textinho bla bla bla"',
                              do_xcom_push=True)
    
    collect_1 = BashOperator(task_id='Coleta_1',
                             bash_command='echo "Valor Retornado: {{ti.xcom_pull(key="numero")}}"')
    
    collect_2 = BashOperator(task_id='Coleta_2',
                             bash_command='echo "Valor Retornado: {{ti.xcom_pull(task_ids=["Python_2"])}}"')
    
    collect_3 = BashOperator(task_id='Coleta_3',
                             bash_command='echo "Valor do teste_bash: {{ti.xcom_pull(task_ids=["bash_1"])}}"',
                             do_xcom_push=False)

    fim = DummyOperator(task_id='Fim')

inicio >> python_1 >> collect_1 >> fim
inicio >> python_2 >> collect_2 >> fim
inicio >> teste_bash >> collect_3 >> fim