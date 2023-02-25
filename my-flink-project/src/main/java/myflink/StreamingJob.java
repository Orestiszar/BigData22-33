package myflink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.*;
import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.*;
import org.apache.flink.table.catalog.hbase.*;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import java.io.IOException;

public class StreamingJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //region Kafka Source datastream
        KafkaSource<Myrow> source = KafkaSource.<Myrow>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("quickstart-new")
                .setGroupId("my-group-flink")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DeserializationSchema<Myrow>() {
                    @Override
                    public Myrow deserialize(byte[] bytes) throws IOException {
                        return Utils.getMapper().readValue(bytes,Myrow.class);
                    }

                    @Override
                    public boolean isEndOfStream(Myrow myrow) {
                        return false;
                    }

                    @Override
                    public TypeInformation<Myrow> getProducedType() {
                        return TypeInformation.of(Myrow.class);
                    }
                })
                .build();
        DataStream<Myrow> dataStream = env.fromSource(source,WatermarkStrategy.noWatermarks(),"Kafka Source");
        //endregion

        tableEnv.createTemporaryView("MyRows", dataStream);

        Table table = tableEnv.sqlQuery("SELECT m_timestamp FROM MyRows");
        TableResult result = table.execute();

        for (CloseableIterator<Row> it = result.collect(); it.hasNext(); ) {
            Row row = it.next();
            System.out.println(row.getField("m_timestamp"));
        }

        DataStream<Row> kafkaSinkStream = tableEnv.toDataStream(table);

        env.execute("Simple SQL query");

        //region Kafka sink
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("hbase-flink")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        dataStream.sinkTo(sink);
        //endregion
    }



    // LEGACY
//    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
//        @Override
//        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
//            for (String word: sentence.split(" ")) {
//                out.collect(new Tuple2<String, Integer>(word, 1));
//            }
//        }
//    }

}