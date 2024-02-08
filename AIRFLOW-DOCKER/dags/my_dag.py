from airflow import DAG
from airflow.aperators.bash import BashOperator #para mandar un echo
from datetime import datetime

with DAG(
    dag_id = "my_dag",
    start_date=datetime(2024,2,7)
) as dag:
    task_1 = BashOperator(
        task_id="saludando",
        bash_command="echo 'hola mundo'"
    )