from airflow.models.baseoperador import BaseOperador
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
#EMAIL
import smtplib
import ssl
from email.message import EmailMessage
#variables de entorno
from airflow.models import Variable

class PostgresFileOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 operation,
                 config={},
                 *args,
                 **kwargs):
        super(PostgresFileOperator, self).__init__(*args, **kwargs)
        self.operation = operation
        self.config = config
        self.postgres_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    
    def execute(self, context):
        if self.operation == "write":
            #escribir en la DB
            self.writeInDb()
        elif self.operation == "read":
            #leer la db
            self.readFromDb()

    def writeInDb(self):
        self.postgres_hook.bulk_load(self.confg.get('table_name'), '/plugins/tmp/file.tsv')


    def readFromDb(self):
        # read from db with a SQL query
        conn = self.postgres_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.config.get("query"))

        data = [doc for doc in cursor]
        print(data)

        if data: #si hay resultados de mi query
            #enviar email
            email_from="anthonycriollo22@gmail.com"
            passw = "123456"  # or Variable.get("password_email")
            email_to = "anthonycriollo22062000@gmail.com"

            title = "ALERTA ! Items con demasiadas ventas"
            body = """
            Hemos detectado nuevos items con demasidas ventas: \n {}
            """.format(data)

            email = EmailMessage()
            email['From'] = email_from
            email['To'] = email_to
            email['Subject'] = title
            email.set_content(body)

            context = ssl.create_default_context()

            with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
                smtp.login(email_from, passw)
                smtp.sendmail(email_from, email_to, email.as_string())



