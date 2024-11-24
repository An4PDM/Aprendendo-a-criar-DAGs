from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
import pprint
import datetime

def funcao2 (**kwargs):
    print('DAG filha!')
    pprint.pprint(kwargs)
    return 'filha'


with DAG (
    dag_id='dag_filha',
    schedule_interval='* 12 * * *',
    start_date=pendulum.datetime(2024,11,24,tz = 'America/Sao_Paulo'),
    end_date=pendulum.datetime(2024,11,25,tz = 'America/Sao_Paulo'),
    catchup=True
) as dag:
    inicio = DummyOperator(task_id = 'inicio')
    wait_principal= ExternalTaskSensor(
        task_id = 'Wait_principal',
        external_dag_id = 'dag_principal',
        external_task_id = 'imprime_1',
        execution_delta =  datetime.timedelta(hours=1),
        poke_interval = 30,
        mode = 'reschedule'
    )
    imprime = PythonOperator(task_id='Imprime_2',python_callable=funcao2)
    fim = DummyOperator(task_id = 'fim')

(inicio >> wait_principal >> imprime >> fim)
