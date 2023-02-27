import os
import subprocess

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import pandas as pd


def main():
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()

    settings = EnvironmentSettings.new_instance()\
                      .in_streaming_mode()\
                      .use_blink_planner()\
                      .build()

    # create table environment
    tbl_env = StreamTableEnvironment.create(stream_execution_environment=env,
                                            environment_settings=settings)

    # add kafka connector dependency
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                            'flink-sql-connector-kafka_2.12-1.13.6.jar')                        
    tbl_env.get_config()\
            .get_configuration()\
            .set_string("pipeline.jars", "file://{}".format(kafka_jar))
           


    #######################################################################
    # Create Kafka Source Table with DDL
    #######################################################################
    src_ddl = """
        CREATE TABLE sensor_data (
            m_name VARCHAR,
            m_value DOUBLE,
            m_timestamp TIMESTAMP,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'quickstart-new',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'sensor_data_group',
            'format' = 'json'
        )
    """

    tbl_env.execute_sql(src_ddl)

    # create and initiate loading of source Table
    tbl = tbl_env.from_path('sensor_data')

    print('\nSource Schema')
    tbl.print_schema()

    #####################################################################
    # Define Tumbling Window Aggregate Calculation (Seller Sales Per Minute)
    #####################################################################
    sql = """
        SELECT
          m_name,
          TUMBLE_END(proctime, INTERVAL '4' SECONDS) AS window_end,
          SUM(m_value) AS window_daily_values
        FROM sensor_data
        GROUP BY
          TUMBLE(proctime, INTERVAL '4' SECONDS),
          m_name
    """

    # sql_sensor_TH1 = f"""
    #     SELECT
    #       m_name,
    #       AVERAGE(m_value) AS daily_average_TH1,
    #       m_timestamp,
    #       TUMBLE_END(proctime, INTERVAL '60' SECONDS) AS window_end,
    #     FROM sensor_data
    #     WHERE m_name = 'TH1' AND 
    # """
    revenue_tbl = tbl_env.sql_query(sql)

    print('\nProcess Sink Schema')
    revenue_tbl.print_schema()

    ###############################################################
    # Create Kafka Sink Table
    ###############################################################
    sink_ddl = """
        CREATE TABLE daily_values (
            m_name VARCHAR,
            window_end TIMESTAMP(3),
            window_daily_values DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales-euros',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """
    tbl_env.execute_sql(sink_ddl)

    # write time windowed aggregations to sink table
    revenue_tbl.execute_insert('daily_values').wait()

    tbl_env.execute('aggregated-daily-values')


if __name__ == '__main__':
    main()