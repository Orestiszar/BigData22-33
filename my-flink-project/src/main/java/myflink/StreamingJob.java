package myflink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.util.UriEncoder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.util.Map;

import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.graalvm.compiler.core.common.util.Util;

public class StreamingJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        KafkaSource<Myrow> source = KafkaSource.<Myrow>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("quickstart")
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


        tableEnv.createTemporaryView("MyRows", dataStream);
        tableEnv.executeSql("SELECT m_name FROM MyRows").print();

//        env.execute("Simple SQL qqqqqqqquery");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}