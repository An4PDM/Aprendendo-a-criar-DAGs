from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
import pendulum

with DAG(
    dag_id="email_n",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 11, 8, 18, 14, tz="America/Sao_Paulo"),
    catchup=True
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    email = EmailOperator(
        task_id="email",
        to="anadesantosdemelo@gmail.com",
        subject="TESTE DE ENVIO",
        html_content="<h3> OLÁ! </h3> <p> Este e-mail foi enviado através do Airflow. </p>"
    )

(start >> email >> end)
