"""Задача №3

Аналитик попросил определить список аэропортов у
которых самые больше проблемы с задержкой на вылет рейса.
Для этого необходимо вычислить среднее, минимальное,
максимальное время задержки и выбрать аэропорты только те
где максимальная задержка (DEPARTURE_DELAY) 1000 секунд и больше.
Дополнительно посчитать корреляцию между временем задержки и днем недели (DAY_OF_WEEK)"""


import argparse

from pathlib import Path
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as f



def process( spark, flights_path, result_path):

    flights_df = spark.read.parquet(flights_path)

    res_df =  (flights_df
        .groupBy(flights_df['ORIGIN_AIRPORT'])
        .agg( f.avg(flights_df['DEPARTURE_DELAY']).alias('avg_delay'),
             f.min(flights_df['DEPARTURE_DELAY']).alias('min_delay'),
             f.max(flights_df['DEPARTURE_DELAY']).alias('max_delay'),
             f.corr('DEPARTURE_DELAY', 'DAY_OF_WEEK').alias('corr_delay2day_of_week'))
        .select(f.col('ORIGIN_AIRPORT'),
                f.col('avg_delay'),
                f.col('min_delay'),
                f.col('max_delay'),
                f.col('corr_delay2day_of_week')))\
        .filter(f.col('max_delay')>=1000)



    #res_df.show()
    res_df.write.mode('overwrite').parquet(result_path)

def main(flights_path, result_path):
    spark = _spark_session()
    process(spark, flights_path,  result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob3').getOrCreate()


if __name__ == "__main__":
    data_path = Path(os.getcwd()).parent.absolute()
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path',
                        type=str,
                        default=str(data_path ) + '/flights.parquet',
                        help='Please set flights datasets path.')

    parser.add_argument('--result_path',
                        type=str,
                        default=str(data_path )+'/spark12_3.parquet',
                        help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)

