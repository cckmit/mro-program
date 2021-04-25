package mro.stream.parser.nokia;

import mro.stream.source.nokia.CollectNokiaFileFtp;
import mro.stream.source.nokia.InitialNokiaData;
import mro.utils.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.util.Map;
import java.util.concurrent.TimeUnit;


public class MrNokiaTransformations {

    public static void main(String[] args) throws Exception {
        /**定义运行环境*/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**启动重启机制*/  //10分钟内失败了5次，重试间隔为15，则失败
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                10,
                Time.of(5, TimeUnit.MINUTES),
                Time.of(15, TimeUnit.SECONDS)));
        /**初始化竞争资源*/
        InitialNokiaData.checkJedis();
        /**广播变量命名定义*/
        final MapStateDescriptor<String, String> CONFIG_KEYWORDS = new MapStateDescriptor<>(
                "config-keywords",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);

        /**自定义广播流，，实现读取Hdfs文件数据下载更新匹配数据*/
        BroadcastStream<Map<String, String>> broadcastStream = env.addSource(new NokiaHdfsSource()).setParallelism(1).broadcast(CONFIG_KEYWORDS);
        /**自定义数据源，实现循环一直下载SFTP文件，内部实现资源竞争*/
        DataStream<Map<String, String>> listdata = env.addSource(new CollectNokiaFileFtp()).setParallelism(89);

        /**filter过滤测试流*/
        SingleOutputStreamOperator<String> map = listdata.connect(broadcastStream)
                .process(new NokiaProcessStream())
                .filter(data -> data.contains("="))
                .name("NokiaProcess");

        /**sink to kafka*/
        map.addSink(new FlinkKafkaProducer010<String>(
                "mro_src_1",
                new MySchema(),
                KafkaUtil.evalKafkaConfigue(),
                new MyPartitioner()
        ));
        env.execute("NOKIA-Station");
    }
}

class MySchema implements KeyedSerializationSchema {
    @Override
    public byte[] serializeKey(Object element) {
        return null;
    }

    @Override
    public byte[] serializeValue(Object element) {
        return element.toString().split("=")[0].getBytes();
    }

    @Override
    public String getTargetTopic(Object element) {
        return null;
    }
}


class MyPartitioner extends FlinkKafkaPartitioner {
    @Override
    public int partition(Object record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        return Integer.parseInt(record.toString().split("=")[1]);
    }

}