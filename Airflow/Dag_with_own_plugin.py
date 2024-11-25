"""    Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location" с полями id, name, type, dimension, resident_cnt.
    ! Обратите внимание: ваш логин в LMS нужно использовать, заменив дефис на нижнее подчёркивание
    С помощью API (https://rickandmortyapi.com/documentation/#location) найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
    Запишите значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents.

> hint

* Для работы с GreenPlum используется соединение 'conn_greenplum_write' в случае, если вы работаете с LMS либо настроить соединение самостоятельно в вашем личном Airflow. Параметры соединения:

* Не забудьте, используя свой логин в LMS, заменить дефис на нижнее подчёркивание

* Можно использовать хук PostgresHook, можно оператор PostgresOperator

* Предпочтительно использовать написанный вами оператор для вычисления top-3 локаций из API

* Можно использовать XCom для передачи значений между тасками, можно сразу записывать нужное значение в таблицу

* Не забудьте обработать повторный запуск каждого таска: предотвратите повторное создание таблицы, позаботьтесь об отсутствии в ней дублей"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging



from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from s_morozova_plugins.s_morozova_ram_location_operator.py import SMorRickMortyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta

def insert_sorted_location (**kwargs):
    loc = kwargs['loc']
    sorted_locations = loc.xcom_pull(task_ids='top_3_locations_data_ram', key='return_value')
    insert_query = """
                        INSERT INTO public.s_morozova_ram_location (id, name, type, dimension, resident_cnt) 
                        VALUES (%s, %s, %s, %s, %s);
                        """
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    for record in sorted_locations:
        pg_hook.run(insert_query, parameters=record)


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's_morozova',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG("s_morozova_lesson_5",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         description="DAG, ищет три локации в сериале Рик и Морти с наибольшим количеством резидентов.",
         max_active_runs=1,
         tags=['s_morozova_5']
         ) as dag:
    start = DummyOperator(
        task_id='start'
    )

    create_location_table = PostgresOperator(
        task_id='create_location_table',
        postgres_conn_id='conn_greenplum_write',
        sql=
        """
        CREATE TABLE IF NOT EXISTS public.s_morozova_ram_location 
            (
            id int4 NOT NULL,
            "name" varchar NULL,
            "type" varchar NULL,
            dimension varchar NULL,
            resident_cnt int4 NULL,
            CONSTRAINT s_morozova_ram_location _pkey PRIMARY KEY (id)
            )
            DISTRIBUTED BY (id);
        """
    )

    clear_location_table = PostgresOperator(
        task_id='clear_location_table',
        postgres_conn_id='conn_greenplum_write',
        sql=
        """
        TRUNCATE TABLE public.s_morozova_ram_location 
        """
    )

    top_3_locations_data_ram = SMorRickMortyOperator(
        task_id='top_3_locations_data_ram',
        loc_num=3
    )

    insert_locations = PythonOperator(
        task_id='insert_locations',
        python_callable = insert_sorted_location,
        dag = dag
    )

    start >> create_location_table >> clear_location_table >> top_3_locations_data_ram >> insert_locations

