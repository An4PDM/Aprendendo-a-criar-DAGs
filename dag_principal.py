from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import pendulum
import pprint

def funcao1(**kwargs):
    print('DAG principal!')
    pprint.pprint(kwargs)
    return 'principal'


with DAG (
    dag_id='dag_principal',
    schedule_interval= '* 11 * * *',
    start_date = pendulum.datetime(2024,11,24,tz='America/Sao_Paulo'),
    end_date = pendulum.datetime(2024,11,25,tz='America/Sao_Paulo'),
    catchup=True
) as dag:

    inicio = DummyOperator(task_id = 'Início')
    imprime = PythonOperator(task_id = 'Imprime_1', python_callable=funcao1)
    fim = DummyOperator(task_id = 'Fim')

(inicio >> imprime >> fim)
