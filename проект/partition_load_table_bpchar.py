from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

LOAD_PART_TABLE_TRAFFIC1 = "select std3_69.partition_load_table_bpchar('std3_69.traffic', 'date', '2021-01-01', '2021-02-01', 'gp.traffic', 'intern', 'intern')"
LOAD_PART_TABLE_TRAFFIC2 = "select std3_69.partition_load_table_bpchar('std3_69.traffic', 'date', '2021-02-01', '2021-03-01', 'gp.traffic', 'intern', 'intern')"
LOAD_PART_TABLE_BILLS1 = "select std3_69.partition_load_table('std3_69.bills_head', 'calday', '2021-01-01', '2021-02-01', 'gp.bills_head', 'intern', 'intern')"
LOAD_PART_TABLE_BILLS2 = "select std3_69.partition_load_table('std3_69.bills_head', 'calday', '2021-02-01', '2021-03-01', 'gp.bills_head', 'intern', 'intern')"

DB_CONN = 'gp_std3_69'
DB_SCHEMA = 'std3_69'
DB_PROC_LOAD = 'full_load_table'
FULL_LOAD_TABLES = ['coupons', 'promo_types', 'promos', 'stores']
FULL_LOAD_FILES = {'coupons': 'coupons', 'promo_types': 'promo_types', 'promos':'promos', 'stores':'stores'}
LOAD_FULL_TABLE = f"select {DB_SCHEMA}.{DB_PROC_LOAD}(%(tab_name)s, %(file_name)s);"
DB_PROC_PLAN = 'load_data'
LOAD_PLAN_FACT = f"select {DB_SCHEMA}.{DB_PROC_PLAN}(%(month1)s, %(month2)s);"


default_args = {
    'depends_on_past': False,
    'owner': 'std3_69',
    'start_date': datetime(2023, 10, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    "std3_69_bills",
    max_active_runs = 3,
    schedule_interval = None,
    default_args = default_args,
    catchup = False,
) as dag:
    
    task_start = DummyOperator(task_id="start")
    
    with TaskGroup("part_insert") as task_part_insert:
        task_part_traffic1 = PostgresOperator(task_id="start_insert_part1_traffic",
                                           postgres_conn_id=DB_CONN,
                                           sql=LOAD_PART_TABLE_TRAFFIC1)
        task_part_traffic2 = PostgresOperator(task_id="start_insert_part2_traffic",
                                           postgres_conn_id=DB_CONN,
                                           sql=LOAD_PART_TABLE_TRAFFIC2)
        task_part_bills1 = PostgresOperator(task_id="start_insert_part1_bills_head",
                                           postgres_conn_id=DB_CONN,
                                           sql=LOAD_PART_TABLE_BILLS1)
        task_part_bills2 = PostgresOperator(task_id="start_insert_part2_bills_head",
                                           postgres_conn_id=DB_CONN,
                                           sql=LOAD_PART_TABLE_BILLS2)
 
    with TaskGroup("full_insert") as task_full_insert:
        for table in FULL_LOAD_TABLES:
            task = PostgresOperator(task_id=f"start_insert_fact_{table}",
                                    postgres_conn_id=DB_CONN,
                                    sql = LOAD_FULL_TABLE,
                                    parameters = {'tab_name':f'{DB_SCHEMA}.{table}',
                                                 'file_name':f'{FULL_LOAD_FILES[table]}'},
                                    )
    task_plan_fact = PostgresOperator(task_id="start_plan_fact",
                                           postgres_conn_id=DB_CONN,
                                           sql=LOAD_PLAN_FACT,
                                           parameters = {'month1':'20210101', 'month2': '20210228'}
                                     )
            
    task_end = DummyOperator(task_id="end")
    
    task_start >> task_part_insert >> task_full_insert >> task_plan_fact >> task_end