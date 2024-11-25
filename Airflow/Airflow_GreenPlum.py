"""Задание

Нужно доработать даг, который вы создали на прошлом занятии.

Он должен:

Работать с понедельника по субботу, но не по воскресеньям (можно реализовать с помощью расписания или операторов ветвления)

Ходить в наш GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри

Используйте соединение 'conn_greenplum' в случае, если вы работаете из LMS либо настройте его самостоятельно в вашем личном Airflow.


    Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)

    Выводить результат работы в любом виде: в логах либо в XCom'е

Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года"""




from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago #здесь был импорт BaseHook
#from airflow.operators.sensors import TimeDeltaSensor
from datetime import timedelta
import logging
import datetime
import functools




DEFAULT_ARGS = {
    'owner': 's-morozova',
    'start_date': days_ago(5),
    'retry_delay': timedelta(minutes=1),
    'sla': timedelta(hours=2),
    'execution_timeout': timedelta(seconds=300),
    'trigger_rule':  'dummy'
}


with DAG ("s_morozova_3_1",
    schedule_interval = '@daily',
    default_args = DEFAULT_ARGS,
    tags =['s_morozova_3_1']
    ) as dag:

    #wait_until_gam = TimeDeltaSensor(
    #    task_id='wait_until_gam',
    #    delta=timedelta(seconds=6 * 60 * 60)
    #)

    end = DummyOperator(
        task_id='end',
        trigger_rule='one_success'
    )

    print_now = BashOperator(
        task_id="print_now",
        bash_command="echo It is currently {{ macros.datetime.now() }}",  # It is currently 2021-08-30 13:51:55.820299
    )

    def day_func():
        logging.info("It's execution_date")


    some_date = PythonOperator(
        task_id='echo_ds',
        python_callable=day_func,  # ссылка на функцию, выполняемую в рамках таски
        dag=dag
    )

    end >> [print_now, some_date]