import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.common import Row
from pyflink.common.serialization import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer



def main():
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()

    env.add_jars("file:///home/orestis/Desktop/BigData22-33/flink_demo/flink-sql-connector-kafka_2.12-1.13.6.jar")

    settings = EnvironmentSettings.new_instance()\
                      .in_streaming_mode()\
                      .build()

    # create table environment
    tbl_env = StreamTableEnvironment.create(stream_execution_environment=env,
                                            environment_settings=settings)
    
    deserialization_schema = JsonRowDeserializationSchema.builder() \
        .type_info(type_info=Types.ROW([Types.LONG(), Types.LONG()])).build()

    kafka_source = FlinkKafkaConsumer(
        topics='test_topic',
        deserialization_schema=deserialization_schema,
        properties={
            'bootstrap.servers':"pkc-w7d6j.germanywestcentral.azure.confluent.cloud:9092",
            'security.protocol':"SASL_SSL",
            'sasl.mechanisms':"PLAIN",
            'sasl.username':"ITVCKFDO6EJFOGHD",
            'sasl.password':"BFmFxMtT1w9ciCxg99DnfI+YWR/Jv+nc4w3X50+4gkOToJJ7yUcRhR1HlkTyVMgM",
            'client.id':'flink-consumer',
            'group.id':'flink-group'
        })

    ds = env.add_source(kafka_source)
    # add kafka connector dependency
    # kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
    #                         'flink-sql-connector-kafka_2.12-1.13.6.jar')


    # tbl_env.get_config()\
    #         .get_configuration()\
    #         .set_string("pipeline.jars", "file://{}".format(kafka_jar))


    #######################################################################
    # Create Kafka Source Table with DDL
    #######################################################################
    src_ddl = """
        CREATE TABLE sensor_input (
            m_name VARCHAR,
            m_value DOUBLE,
            m_timestamp VARCHAR
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'test_topic',
            'properties.group.id' = 'testgroup',
            'properties.client.id'='testclient',
            'properties.bootstrap.servers'='pkc-w7d6j.germanywestcentral.azure.confluent.cloud:9092',
            'properties.security.protocol'='SASL_SSL',
            'properties.sasl.mechanisms'='PLAIN',
            'properties.sasl.username'='ITVCKFDO6EJFOGHD',
            'properties.sasl.password'='BFmFxMtT1w9ciCxg99DnfI+YWR/Jv+nc4w3X50+4gkOToJJ7yUcRhR1HlkTyVMgM',
            'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="ITVCKFDO6EJFOGHD" password="BFmFxMtT1w9ciCxg99DnfI+YWR/Jv+nc4w3X50+4gkOToJJ7yUcRhR1HlkTyVMgM" serviceName="kafka";',
            'format' = 'json'
        )
    """

    tbl_env.execute_sql(src_ddl)

    # create and initiate loading of source Table
    tbl = tbl_env.from_path('sensor_input')

    print('\nSource Schema')
    tbl.print_schema()

    #####################################################################
    # Define Tumbling Window Aggregate Calculation (Seller Sales Per Minute)
    #####################################################################
    sql = """
        SELECT
          m_name,
          m_value,
          m_timestamp
        FROM sensor_input
    """
    revenue_tbl = tbl_env.sql_query(sql)

    print('\nProcess Sink Schema')
    revenue_tbl.print_schema()

    ###############################################################
    # Create Sink Table
    ###############################################################
    sink_ddl = """
        CREATE TABLE output_print (
            m_name VARCHAR,
            m_val DOUBLE,
            m_timestamP VARCHAR
        ) WITH (
            'connector'='print'
        )
    """
    tbl_env.execute_sql(sink_ddl)

    # write time windowed aggregations to sink table
    revenue_tbl.execute_insert('output_print').wait()

    tbl_env.execute('windowed-output-print')

if __name__ == '__main__':
    main()