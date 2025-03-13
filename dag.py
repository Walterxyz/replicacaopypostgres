import airflow
from datetime import datetime, timedelta  
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 9),
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    "params": []
}

ids = {
    'Replicacao_industria': 'industria1',
    'Replicacao_industria': 'industria2',
}

def cria_dag(key, value):
    with DAG(
        dag_id=key,
        default_args=default_args,
        tags=key.split('_'),
        max_active_runs=1,
        catchup=False,
        schedule=timedelta(seconds=15),
    ) as dag:

        t0 = BashOperator(
            task_id=value+'_leitura', 
            bash_command=f'python3.11 /opt/airflow/dags/scripts/replicacao_leitura.py {value}',
            retries=5,
            retry_delay=timedelta(seconds=5),
            dag=dag
        )
        
        t1 = BashOperator(
            task_id=value+'_escrita',
            bash_command=f'python3.11 /opt/airflow/dags/scripts/replicacao_escrita.py {value}', 
            retries=5,
            retry_delay=timedelta(seconds=5),
            dag=dag
        )

        return dag

for key, value in ids.items():
    globals()[key] = cria_dag(key, value)
