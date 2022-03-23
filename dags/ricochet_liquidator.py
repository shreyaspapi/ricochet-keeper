from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from blocksec_plugin.web3_hook import Web3Hook
from blocksec_plugin.ethereum_block_to_postgres_operator import EthereumBlocktoPostgresOperator
from blocksec_plugin.ethereum_events_to_postgres_operator import EthereumEventstoPostgresOperator
from blocksec_plugin.abis import REX_ABI
from datetime import datetime, timedelta
from json import loads

SCHEDULE_INTERVAL = Variable.get("liquidator-schedule-interval", "*/15 * * * *")
REX_BANK_ADDRESSES = Variable.get("rex-bank-addresses", deserialize_json=True)
REX_BANK_LIQUIDATOR_ADDRESS = Variable.get("rex-bank-liquidator-address")

default_args = {
    "owner": "ricochet",
    "depends_on_past": False,
    "start_date": datetime(2021, 8, 1, 23, 0),
    "email": ["mike@mikeghen.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1)
}


dag = DAG("ricochet_liquidator", catchup=False, default_args=default_args, schedule_interval=SCHEDULE_INTERVAL)

def get_from_block_height(**context):
    """
    Check the smart_contracts table for the current_block_height
    """
    execution_date = context['execution_date'].isoformat()
    sql = """
    SELECT block_height
    FROM ethereum_blocks
    WHERE mined_at < ('{0}'::timestamp -  interval '1 hour')
    ORDER BY 1 DESC
    LIMIT 1
    """.format(execution_date)
    print(sql)
    postgres = PostgresHook(postgres_conn_id='data_warehouse')
    conn = postgres.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    try:
        from_block_height = result[0][0]
    except IndexError:
        # first time running
        from_block_height = 17498383
    return from_block_height

done = BashOperator(
    task_id='done',
    bash_command='date',
    dag=dag,
)

get_to_block_height = EthereumBlocktoPostgresOperator(
    task_id="get_to_block_height",
    postgres_conn_id='data_warehouse',
    postgres_table='ethereum_blocks',
    web3_conn_id='infura',
    dag=dag,
)

get_from_block_height = PythonOperator(
    task_id="get_from_block_height",
    provide_context=True,
    python_callable=get_from_block_height,
    dag=dag
)

for bank_address in REX_BANK_ADDRESSES:

    extract_events = EthereumEventstoPostgresOperator(
        task_id="record_events_"+exchange_address,
        postgres_conn_id='data_warehouse',
        postgres_table='ethereum_events',
        abi_json=loads(REX_ABI),
        contract_address=exchange_address,
        from_block="{{task_instance.xcom_pull(task_ids='get_from_block_height')}}",
        to_block="{{task_instance.xcom_pull(task_ids='get_to_block_height')}}",
        web3_conn_id='infura',
        dag=dag,
    )

    extract_events << get_to_block_height
    extract_events << get_from_block_height
    done << extract_events
