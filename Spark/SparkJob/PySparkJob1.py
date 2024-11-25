from pyspark.shell import spark

import argparse
from pathlib import Path
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType,IntegerType


dim_columns = ['YEAR', 'MONTH', 'DAY,''DAY_OF_WEEK','AIRLINE','FLIGHT_NUMBER',
               'TAIL_NUMBER','ORIGIN_AIRPORT,''DESTINATION_AIRPORT','SCHEDULED_DEPARTURE',
               'DEPARTURE_TIME','DEPARTURE_DELAY','TAXI_OUT','WHEELS_OFF','SCHEDULED_TIME',
               'ELAPSED_TIME','AIR_TIME','DISTANCE','WHEELS_ON','TAXI_IN','SCHEDULED_ARRIVAL',
               'ARRIVAL_TIME','ARRIVAL_DELAY','DIVERTED','CANCELLED','CANCELLATION_REASON',
               'AIR_SYSTEM_DELAY','SECURITY_DELAY','AIRLINE_DELAY','LATE_AIRCRAFT_DELAY',
               'WEATHER_DELAY'
                ]
flights_schema = StructType([
    StructField('YEAR', IntegerType(), True),
    StructField('MONTH',IntegerType(), True),
    StructField('DAY'	,IntegerType(), True),
    StructField('DAY_OF_WEEK'	,IntegerType(), True),
    StructField('AIRLINE'	, StringType(), True),
    StructField('FLIGHT_NUMBER'	, StringType(), True),
    StructField('TAIL_NUMBER'	, StringType(), True),
    StructField('ORIGIN_AIRPORT'	, StringType(), True),
    StructField('DESTINATION_AIRPORT'	, StringType(), True),
    StructField('SCHEDULED_DEPARTURE'	,IntegerType(), True),
    StructField('DEPARTURE_TIME'	,IntegerType(), True),
    StructField('DEPARTURE_DELAY'	,IntegerType(), True),
    StructField('TAXI_OUT'	,IntegerType(), True),
    StructField('WHEELS_OFF'	,IntegerType(), True),
    StructField('SCHEDULED_TIME'	,IntegerType(), True),
    StructField('ELAPSED_TIME'	,IntegerType(), True),
    StructField('AIR_TIME'	,IntegerType(), True ),
    StructField('DISTANCE'	,IntegerType(), True),
    StructField('WHEELS_ON'	,IntegerType(), True),
    StructField('TAXI_IN'	,IntegerType(), True),
    StructField('SCHEDULED_ARRIVAL'	,IntegerType(), True),
    StructField('ARRIVAL_TIME'	,IntegerType(), True),
    StructField('ARRIVAL_DELAY'	,IntegerType(), True),
    StructField('DIVERTED'	,IntegerType(), True),
    StructField('CANCELLED'	,IntegerType(), True),
    StructField('CANCELLATION_REASON'	, StringType(), True),
    StructField('AIR_SYSTEM_DELAY'	,IntegerType(), True),
    StructField('SECURITY_DELAY'	,IntegerType(), True),
    StructField('AIRLINE_DELAY'	,IntegerType(), True),
    StructField('LATE_AIRCRAFT_DELAY'	,IntegerType(), True),
    StructField('WEATHER_DELAY'	,IntegerType(), True)
])

data_path =  Path(os.getcwd()).parent.absolute()

def process(spark, flights_path, result_path):
    #flights_path = os.path.join(Path(__name__).parent, './practice4/data', '*.parquet')
    flights_df = spark.read.parquet(flights_path)

    res_df =  flights_df \
        .where(flights_df['TAIL_NUMBER'].isNotNull()) \
        .groupBy( flights_df['TAIL_NUMBER']) \
        .agg(f.count( flights_df['TAIL_NUMBER']).alias('count') )\
        .select(f.col('TAIL_NUMBER'),
                f.col('count'))\
        .orderBy(f.col('count').desc())\
        .limit(10)

    #return datamart
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
    return SparkSession.builder.master('local').appName('spark12_1').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path',
                        type=str,
                        default=str(data_path ) + '/flights.parquet',
                        help='Please set flights datasets path.')
    parser.add_argument('--result_path',
                        type=str,
                        default=str(data_path )+'/spark12_1.parquet',
                        help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)

