"""Для дашборда с отображением выполненных рейсов требуется собрать таблицу на основе наших данных.
Никакой фильтрации данных не требуется.

Требуемые поля:
Поле	Описание
AIRLINE_NAME	Название авиалинии (airlines.AIRLINE)
TAIL_NUMBER	Номер рейса (flights.TAIL_NUMBER)
ORIGIN_COUNTRY	Страна отправления (airports.COUNTRY)
ORIGIN_AIRPORT_NAME	Полное название аэропорта отправления (airports.AIRPORT)
ORIGIN_LATITUDE	Широта аэропорта отправления (airports.LATITUDE)
ORIGIN_LONGITUDE	Долгота аэропорта отправления (airports.LONGITUDE)
DESTINATION_COUNTRY	Страна прибытия (airports.COUNTRY)
DESTINATION_AIRPORT_NAME	Полное название аэропорта прибытия (airports.AIRPORT)
DESTINATION_LATITUDE	Широта аэропорта прибытия (airports.LATITUDE)
DESTINATION_LONGITUDE	Долгота аэропорта прибытия (airports.LONGITUDE)

Используйте за основу следующие шаблоны:

1) PySparkJob4.py - шаблон для задачи процесса преобразования данных.
Параметры запуска задачи:
flights_path - путь к файлу с данными о авиарейсах
airlines_path - путь к файлу с данными о авиалиниях
airports_path - путь к файлу с данными о аэропортах
result_path - путь куда будет сохранен результат
Подсказки: join, withColumnRenamed, withColumn"""

import argparse
from pathlib import Path
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def process(spark, flights_path,airlines_path,airports_path, result_path):
        flights_df = (spark.read
                    .options(inferSchema = 'true', header = 'true')
                    .parquet(flights_path))
        airlines_df = (spark.read
                    .options(inferSchema = 'true', header = 'true')
                    .parquet(airlines_path)) \
                    .withColumnRenamed('AIRLINE', 'AIRLINE_NAME') \

        airports_df = (spark.read
                    .options(inferSchema = 'true', header = 'true')
                    .parquet(airports_path)) \
                    .withColumnRenamed("IATA_CODE","ORIGIN_IATA_CODE")


        airports_df_destination = airports_df.alias("airports_df_destination")\
            .withColumn({
                "DESTINATION_COUNTRY": "COUNTRY",
                "DESTINATION_AIRPORT_NAME":"AIRPORT",
                "DESTINATION_LATITUDE":"LATITUDE",
                "DESTINATION_LONGITUDE":"LONGITUDE"})\
            .withColumnRenamed('ORIGIN_IATA_CODE','DESTINATION_IATA_CODE')
        # flights_df.show(5)
        # airlines_df.show(5)
        # airports_df.show(5)
        # airports_df_destination.show(5)
        res_df = flights_df\
            .join(other = airlines_df, on = flights_df.AIRLINE == airlines_df.IATA_CODE, how = 'inner')\
            .join(other = airports_df , on = flights_df.ORIGIN_AIRPORT == airports_df.ORIGIN_IATA_CODE, how = 'inner') \
            .withColumnsRenamed({
                            "COUNTRY": "ORIGIN_COUNTRY",
                            "AIRPORT": "ORIGIN_AIRPORT_NAME",
                            "LATITUDE": "ORIGIN_LATITUDE",
                            "LONGITUDE": "ORIGIN_LONGITUDE"}) \
            .select(f.col('DESTINATION_AIRPORT'),
                    f.col('TAIL_NUMBER'),
                    f.col('ORIGIN_COUNTRY'),
                    f.col('ORIGIN_AIRPORT_NAME'),
                    f.col('ORIGIN_LATITUDE'),
                    f.col('ORIGIN_LONGITUDE'),
                    f.col('AIRLINE_NAME'))

        res_df= (res_df
            .join(other = airports_df_destination, on =flights_df.DESTINATION_AIRPORT == airports_df_destination.DESTINATION_IATA_CODE, how ='inner')) \
            .select(f.col('AIRLINE_NAME'),
                    f.col('TAIL_NUMBER'),
                    f.col('ORIGIN_COUNTRY'),
                    f.col('ORIGIN_AIRPORT_NAME'),
                    f.col('ORIGIN_LATITUDE'),
                    f.col('ORIGIN_LONGITUDE'),
                    f.col('DESTINATION_COUNTRY'),
                    f.col('DESTINATION_AIRPORT_NAME'),
                    f.col('DESTINATION_LATITUDE'),
                    f.col('DESTINATION_LONGITUDE' ))


        #res_df.show()
        res_df.write.mode('overwrite').parquet(result_path)

def main(flights_path,airlines_path,airports_path, result_path):
    spark = _spark_session()
    process(spark, flights_path,airlines_path,airports_path,  result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob4').getOrCreate()


if __name__ == "__main__":
    data_path = Path(os.getcwd()).parent.absolute()
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path',
                        type=str,
                        default=str(data_path) + '/flights.parquet',
                        help='Please set flights datasets path.')
    parser.add_argument('--airlines_path',
                        type=str,
                        default=str(data_path) + '/airlines.parquet',
                        help='Please set airlines datasets path.')
    parser.add_argument('--airports_path',
                        type=str,
                        default=str(data_path) + '/airports.parquet',
                        help='Please set airports datasets path.')
    parser.add_argument('--result_path',
                        type=str,
                        default=str(data_path) + '/spark12_4.parquet',
                        help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    airports_path = args.airports_path
    result_path = args.result_path
    main(flights_path,airlines_path,airports_path,result_path)

