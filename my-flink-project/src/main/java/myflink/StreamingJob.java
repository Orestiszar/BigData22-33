package myflink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

import java.io.IOException;

public class StreamingJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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


        tableEnv.createTemporaryView("MyRows", dataStream);
//        tableEnv.executeSql("SELECT m_name FROM MyRows");

        TableResult result = tableEnv.executeSql("SELECT m_timestamp FROM MyRows");

        for (CloseableIterator<Row> it = result.collect(); it.hasNext(); ) {
            Row row = it.next();
            System.out.println(row.getField("m_timestamp"));
        }

//        String temp_hbase = "CREATE TABLE hTable (\n" +
//                " rowkey INT,\n" +
//                " cf ROW<q1 STRING, q2 STRING, q3 STRING>,\n" +
//                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
//                ") WITH (\n" +
//                " 'connector' = 'hbase-2.2',\n" +
//                " 'table-name' = 'test',\n" +
//                " 'zookeeper.quorum' = 'localhost:2181'\n" +
//                ");";
//
//        tableEnv.executeSql(temp_hbase);
//        String temp_insert = "INSERT INTO hTable\n" +
//                "(rowkey, cf) VALUES ('1', 'test');\n";
//        tableEnv.executeSql(temp_insert);


//        TODO: check if needed
        // Instantiating configuration class
        Configuration con = HBaseConfiguration.create();

        // Instantiating HbaseAdmin class
        HBaseAdmin admin = new HBaseAdmin(con);

        // Instantiating table descriptor class
        HTableDescriptor tableDescriptor = new
                HTableDescriptor(TableName.valueOf("emp"));

        // Adding column families to table descriptor
        tableDescriptor.addFamily(new HColumnDescriptor("personal"));
        tableDescriptor.addFamily(new HColumnDescriptor("professional"));

        // Execute the table through admin
        admin.createTable(tableDescriptor);
        System.out.println(" Table created ");



        env.execute("Simple SQL query");


    }
   /* public static void main(String[] args) {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create a sample stream of data as Tuple2<String, Integer>
        DataStream<Tuple2<String, Integer>> stream = env.fromElements(
                new Tuple2<>("key1", 10),
                new Tuple2<>("key2", 20),
                new Tuple2<>("key3", 30));

        // set up the HBaseTableSchema for the HBase table
        HBaseTableSchema schema = new HBaseTableSchema();
        schema.addColumn("cf1", "key", Bytes.toBytes("UTF-8"));
        schema.addColumn("cf1", "value", Bytes.toBytes("UTF-8"));

        // set up the HBaseTableDescriptor for the HBase table
        HTableDescriptor tableDescriptor = new HTableDescriptor(Table"example_table", schema);

        // set up the HBaseSinkFunction to write data to the HBase table
        HBaseSinkFunction<Tuple2<String, Integer>> hbaseSinkFunction = new HBaseSinkFunction<>(
                tableDescriptor,
                new SerializableHBaseFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void apply(Tuple2<String, Integer> value, Put put) {
                        // set the row key
                        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("key"), Bytes.toBytes(value.f0));

                        // set the value
                        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("value"), Bytes.toBytes(value.f1));
                    }
                },
                new RowDataConverter());

        // write the stream data to the HBase table using HBaseSink
        stream.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                return value;
            }
        }).addSink(new HBaseSink<>(hbaseSinkFunction));

        // execute the Flink program
        env.execute("Flink HBase Example");
    }*/
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