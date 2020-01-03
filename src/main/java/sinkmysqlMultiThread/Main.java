package sinkmysqlMultiThread;

import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import java.util.Properties;

public class Main {

    public static void main(String[] args) throws Exception{
        //1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2182");
        props.put("group.id", "test-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        SingleOutputStreamOperator<Student> studentDataStreamSource = env.addSource(new FlinkKafkaConsumer011<>(
                                                                            "test",
                                                                            new SimpleStringSchema(),
                                                                            props))
                                                         .map(str -> new Gson().fromJson(str, Student.class));;


        //3.显示结果
        studentDataStreamSource.addSink(new SinkToMySQLMultiThread());

        //4.触发流执行
        env.execute();
    }

}
