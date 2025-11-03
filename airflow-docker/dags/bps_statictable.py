# dags/bps_etl_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import semua 19 fungsi dari folder statictable
from scripts.statictable.api_1 import run_etl_kode_1
from scripts.statictable.api_2 import run_etl_kode_2
# from bps_scripts.statictable.kode_3 import run_etl_kode_3
# ... (import 16 lainnya)
# from bps_scripts.statictable.kode_19 import run_etl_kode_19

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='bps_statictable_etl_19_tasks',
    default_args=default_args,
    description='DAG untuk mengambil 19 tabel statis BPS secara paralel',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bps', 'etl', 'statictable']
) as dag:

    # TASK 1
    task_1 = PythonOperator(
        task_id='etl_tabel_Overview_Kependudukan',  # (Nama unik untuk task 1)
        python_callable=run_etl_kode_1  # (Fungsi yang dipanggil)
    )

    # TASK 2
    task_2 = PythonOperator(
        task_id='etl_tabel_Overview_Pendidikan',  # (Nama unik untuk task 2)
        python_callable=run_etl_kode_2  # (Fungsi yang dipanggil)
    )

    # TASK 3 (Contoh)
    # task_3 = PythonOperator(
    #     task_id='etl_tabel_XXXX',  # (Ganti dengan nama/id tabel)
    #     python_callable=run_etl_kode_3
    # )

    # ...
    # ... Buat task 4 sampai 19 dengan pola yang sama ...
    # ...

    # TASK 19 (Contoh)
    # task_19 = PythonOperator(
    #     task_id='etl_tabel_YYYY',
    #     python_callable=run_etl_kode_19
    # )

    # Saat ini, tidak ada dependensi antar task (task_1 >> task_2).
    # Ini berarti Airflow akan menjalankan semua 19 task secara BERSAMAAN (paralel).
    # Ini JAUH LEBIH CEPAT.
    
    # Jika Anda ingin menjalankannya satu per satu (urut):
    # task_1 >> task_2 >> task_3 ... >> task_19