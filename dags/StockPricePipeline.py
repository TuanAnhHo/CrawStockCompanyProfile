# from airflow.operators.python import PythonOperator,BranchPythonOperator
from src import insert_company_profile

A = insert_company_profile("ACB", '2024-01-22', '2024-01-26')
print(A)

# # Declare default arguments in Airflow
# default_args = {
#     'owner':'anh.ho',
#     'start_date':'2024-01-01',
#     'email':'anh.ho@tititada.com',
#     'retries':0,
# }
#
# dag= DAG(
#     dag_id="company_profile_pipeline",
#     default_args=default_args,
#     schedule='@daily',
#     catchup=True
# )
#
# brand_check_existing_table = BranchPythonOperator(
#     task_id='CheckTableExisting',
#     python_callable=CheckTableExisting,
#     op_kwargs = {'table_name':table_name},
#     dag=dag,
# )
#
# branch_check_existing_profile = BranchPythonOperator(
#     task_id='ExistingCompanyProfile',
#     python_callable=ExistingCompanyProfile,
#     op_kwargs = {'symbol':symbol},
#     dag=dag,
# )
#
# create_table = PythonOperator(
#     task_id = "CreateCompanyProfileTable",
#     python_callable=CreateCompanyProfileTable,
#     dag=dag,
# )
#
# insert_data1 = PythonOperator(
#     task_id = "InsertCompanyProfile_branch_1",
#     python_callable=InsertCompanyProfile,
#     op_kwargs = {'symbol':symbol},
#     dag=dag,
# )

# insert_data2 = PythonOperator(
#     task_id = "InsertCompanyProfile_branch_2",
#     python_callable=InsertCompanyProfile,
#     op_kwargs = {'symbol':symbol},
#     dag=dag,
# )
#
# update_data = PythonOperator(
#     task_id = "UpdateCompanyProfile",
#     python_callable=UpdateCompanyProfile,
#     op_kwargs = {'symbol':symbol},
#     dag=dag,
# )
#
# brand_check_existing_table >> [create_table,branch_check_existing_profile]
# create_table >> insert_data1
# branch_check_existing_profile >> [insert_data2, update_data]
