from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
import requests
from airflow.models import Variable
import pandas as pd
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.apprise.notifications.apprise import send_apprise_notification
from apprise import NotifyType


PATH_ROOT_DATA = "/opt/airflow/data/"
PATH_OLD_DATA = "old/"
PATH_NEW_DATA = "new/"
FILE_NAME = "london_houses.csv"
NEW_FILE_NAME = "london_houses_ThaiBaht.csv"
PATH_FILE = PATH_ROOT_DATA + PATH_OLD_DATA + FILE_NAME
PATH_NEW_FILE = PATH_ROOT_DATA + PATH_NEW_DATA + NEW_FILE_NAME

S3_URL = "s3://london-houses/"
FILE_NAME_IN_S3 = S3_URL + NEW_FILE_NAME

S3_CONN_ID = "aws_con"
APPRISE_CONN_ID = "notifier"


def hello():
    print("Hello from london")

def getExchangeRate(ti):
    BASE_URL = "https://v6.exchangerate-api.com/v6/"
    API_KEY = Variable.get("API_KEY")
    CURRENCY = "GBP"
    LATEST = "/latest/"

    URL = BASE_URL + API_KEY + LATEST + CURRENCY

    response = requests.get(URL).json()

    print(response["conversion_rates"]["THB"])
    ti.xcom_push(key="GBPTHB",value=response["conversion_rates"]["THB"])



def readCsvfile(ti):
    df = pd.read_csv(PATH_FILE)
    GBPTHB = ti.xcom_pull(task_ids="getExchangeRate", key="GBPTHB")
    df['Price in Thai baht'] = df['Price (£)'] * GBPTHB
    newDf = df.drop('Price (£)', axis=1)
    newDf.to_csv(PATH_NEW_FILE)
    print("df: ",df)

default_args = {
    "owner": "ongg",
    "start_date": timezone.datetime(2022, 2, 1),
}
with DAG(
    "london_houses",
    default_args=default_args,
    schedule_interval=None,
    on_failure_callback=send_apprise_notification(
        title='Airflow Task Failed',
        body='The @dag {{ ti.dag_id }} failed',
        notify_type=NotifyType.FAILURE,
        apprise_conn_id=APPRISE_CONN_ID,
        tag='alerts'
    ),
    on_success_callback=send_apprise_notification(
        title='Airflow Task Success',
        body='The @dag  {{ ti.dag_id }} succeeded',
        notify_type=NotifyType.SUCCESS,
        apprise_conn_id=APPRISE_CONN_ID,
        tag='alerts'
    )
) as dag:
  
    hello = PythonOperator(
      task_id="hello",
      python_callable=hello,
    )

    getExchangeRate = PythonOperator(
        task_id="getExchangeRate",
        python_callable=getExchangeRate,
        dag=dag
    )

    readCsvfile = PythonOperator(
        task_id="readCsvfile",
        python_callable=readCsvfile,
        dag=dag
    )

    upload_file = LocalFilesystemToS3Operator(
         task_id = "upload_file",
         filename=  PATH_NEW_FILE,
         dest_key= FILE_NAME_IN_S3,
         aws_conn_id = S3_CONN_ID,
         replace= True
     )

    hello >> getExchangeRate >> readCsvfile >> upload_file


  
      
    