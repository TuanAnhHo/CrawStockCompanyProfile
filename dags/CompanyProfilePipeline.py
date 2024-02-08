from src.CrawlCompanyInfo import *
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
import pytz

config_symbol = "HPG"

# Declare default arguments in Airflow
default_args = {
    'owner':'anh.ho',
    'start_date':str(datetime.now(tz=pytz.timezone("Asia/Ho_Chi_Minh"))),
    'email':'anh.ho@tititada.com',
    'retries':0,
}

dag1= DAG(
    dag_id="company_profile_pipeline",
    default_args=default_args,
    schedule='@daily',
    catchup=True
)

brand_check_existing_table = BranchPythonOperator(
    task_id='CheckExistingCompanyProfile',
    python_callable=check_existing_company_profile,
    op_kwargs = {'symbol': config_symbol},
    dag=dag1,
)

insert_data = PythonOperator(
    task_id = "InsertCompanyProfile",
    python_callable=insert_company_profile,
    op_kwargs={'symbol': config_symbol},
    dag=dag1,
)

update_data = PythonOperator(
    task_id = "UpdateCompanyProfile",
    python_callable=update_company_profile,
    op_kwargs={'symbol': config_symbol},
    dag=dag1,
)

brand_check_existing_table >> [insert_data,update_data]