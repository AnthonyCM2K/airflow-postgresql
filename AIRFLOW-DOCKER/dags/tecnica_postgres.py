from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

from operators.PostgresFileOperator import PostgresFileOperator
from airflow.models.baseoperador import BaseOperador

with DAG(
    dag_id="tecnica_postgres",
    default_args=default_args,
    start_date=datetime(2024,2,7),
    #schedule_interval='0 0 * * *'
) as dag:
    task_1 = PostgresOperator(
        task_id="Crear_tabla",
        postgres_conn_id = "postgres_localhost",
        sql="""
            CREATE TABLE IF NOT EXIST tecnica_ml (
                id varchar(30),
                site_id varchar(100),
                title varchar(100),
                price varchar(100),
                sold_quantily varchar(100),
                thumbnail varchar(200),
                created_date varchar(8)
                primary key(id,created_date)
            ) 
        """
    ),
    task_2 = BashOperator(
        task_id = "Consulting_API",
        bash_command = "python /opt/airflow/plugins/tmp/consult_api.py" #validar rutas
    ),
    task_3 = PostgresFileOperator(
        task_id="Insertar_Data",
        operation="write",
        config={"table_name":"tecnica_ml"}
    ),
    task_4 = PostgresFileOperator(
        task_id ="Reading_Data",
        operation="read",
        config={"query": "SELECT * FROM tecnica_ml WHERE sold_quantity != 'null' and created_date={DATE} and cast(price as decimal) * cast(sold_quantity as int) > 7000000 "}
    )


 # DEPENDENCIAS
    task_1 >> task_2 >> task_3