import os
import subprocess

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.udf import udf

import pandas as pd
from threading import Thread
import DataCreation


def main():
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()

    settings = EnvironmentSettings.new_instance()\
                      .in_streaming_mode()\
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
            'topic' = 'input',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'sensor_data_group',
            'format' = 'json'
        )
    """

    data_creation_thread = Thread(target = DataCreation.create_data)
    tbl_env.execute_sql(src_ddl)
    data_creation_thread.start()

    # create and initiate loading of source Table
    tbl = tbl_env.from_path('sensor_data')

    print('\nSource Schema')
    tbl.print_schema()

    #####################################################################
    #                           Define Aggregations
    #####################################################################

    sql_sensor_TH1 = """
        SELECT
          m_name,
          MAX(m_timestamp) as the_timestamp,
          AVG(m_value) AS window_daily_values
        FROM sensor_data
        WHERE m_name = 'TH1'
        GROUP BY
          TUMBLE(proctime, INTERVAL '96' SECONDS),
          m_name
    """

    sql_sensor_TH2 = """
        SELECT
          m_name,
          MAX(m_timestamp) as the_timestamp,
          AVG(m_value) AS window_daily_values
        FROM sensor_data
        WHERE m_name = 'TH2'
        GROUP BY
          TUMBLE(proctime, INTERVAL '96' SECONDS),
          m_name
    """

    sql_sensor_HVAC1 = """
        SELECT
          m_name,
          MAX(m_timestamp) as the_timestamp,
          SUM(m_value) AS window_daily_values
        FROM sensor_data
        WHERE m_name = 'HVAC1'
        GROUP BY
          TUMBLE(proctime, INTERVAL '96' SECONDS),
          m_name
    """
    sql_sensor_HVAC2 = """
        SELECT
          m_name,
          MAX(m_timestamp) as the_timestamp,
          SUM(m_value) AS window_daily_values
        FROM sensor_data
        WHERE m_name = 'HVAC2'
        GROUP BY
          TUMBLE(proctime, INTERVAL '96' SECONDS),
          m_name
    """
    sql_sensor_W1 = """
        SELECT
          m_name,
          MAX(m_timestamp) as the_timestamp,
          SUM(m_value) AS window_daily_values
        FROM sensor_data
        WHERE m_name = 'W1'
        GROUP BY
          TUMBLE(proctime, INTERVAL '96' SECONDS),
          m_name
    """
    
    sql_sensor_MIAC1 = """
        SELECT
          m_name,
          MAX(m_timestamp) as the_timestamp,
          SUM(m_value) AS window_daily_values
        FROM sensor_data
        WHERE m_name = 'MiAC1'
        GROUP BY
          TUMBLE(proctime, INTERVAL '96' SECONDS),
          m_name
    """
    
    sql_sensor_MIAC2 = """
        SELECT
          m_name,
          MAX(m_timestamp) as the_timestamp,
          SUM(m_value) AS window_daily_values
        FROM sensor_data
        WHERE m_name = 'MiAC2'
        GROUP BY
          TUMBLE(proctime, INTERVAL '96' SECONDS),
          m_name
    """

    sql_sensor_MOV1 = """
        SELECT
          m_name,
          MAX(m_timestamp) as the_timestamp,
          SUM(m_value) AS window_daily_values
        FROM sensor_data
        WHERE m_name = 'MOV1'
        GROUP BY
          TUMBLE(proctime, INTERVAL '96' SECONDS),
          m_name
    """

    sql_raw_data = """
        SELECT
          m_name AS m_name,
          m_timestamp,
          m_value
        FROM sensor_data
        """


    ###############################################################
    #        EXECUTE THE QUERIES AND SAVE THEM TO VARIABLES
    ###############################################################


    TH1_tbl = tbl_env.sql_query(sql_sensor_TH1)
    TH2_tbl = tbl_env.sql_query(sql_sensor_TH2)
    HVAC1_tbl = tbl_env.sql_query(sql_sensor_HVAC1)
    HVAC2_tbl = tbl_env.sql_query(sql_sensor_HVAC2)
    MIAC1_tbl = tbl_env.sql_query(sql_sensor_MIAC1)
    MIAC2_tbl = tbl_env.sql_query(sql_sensor_MIAC2)
    W1_tbl = tbl_env.sql_query(sql_sensor_W1)
    MOV1_tbl = tbl_env.sql_query(sql_sensor_MOV1)
    raw_tbl = tbl_env.sql_query(sql_raw_data)

    print('\nProcess Sink Schema')
    # revenue_tbl.print_schema()

    ###############################################################
    # Create The Kafka Sink Tables
    ###############################################################
    sink_ddl = """
        CREATE TABLE daily_values (
            m_name VARCHAR,
            the_timestamp TIMESTAMP,
            window_daily_values DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'output_aggr',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """
    tbl_env.execute_sql(sink_ddl)

    sink_ddl_raw = """
        CREATE TABLE daily_values_raw (
            m_name VARCHAR,
            m_timestamp TIMESTAMP,
            m_value DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'output_raw',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """
    tbl_env.execute_sql(sink_ddl_raw)

    statement_set = tbl_env.create_statement_set()

    # emit the "table" object to the "first_sink_table"
    statement_set.add_insert("daily_values", TH1_tbl)
    statement_set.add_insert("daily_values_raw", raw_tbl)

    # emit the "simple_source" to the "second_sink_table" via a insert sql query
    statement_set.add_insert("daily_values", TH2_tbl)
    statement_set.add_insert("daily_values", HVAC1_tbl)
    statement_set.add_insert("daily_values", HVAC2_tbl)
    statement_set.add_insert("daily_values", MIAC1_tbl)
    statement_set.add_insert("daily_values", MIAC2_tbl)
    statement_set.add_insert("daily_values", W1_tbl)
    statement_set.add_insert("daily_values", MOV1_tbl)

    # execute the statement set
    statement_set.execute().wait()



    # write time windowed aggregations to sink table
    # TH1_tbl.execute_insert('daily_values')
    # TH2_tbl.execute_insert('daily_values')
    # HVAC1_tbl.execute_insert('daily_values')
    # HVAC2_tbl.execute_insert('daily_values')
    # MIAC1_tbl.execute_insert('daily_values')
    # MIAC2_tbl.execute_insert('daily_values')
    # W1_tbl.execute_insert('daily_values')
    # MOV1_tbl.execute_insert('daily_values')
    # raw_tbl.execute_insert('daily_values_raw').wait()


    tbl_env.execute('aggregated-daily-values')


if __name__ == '__main__':
    main()