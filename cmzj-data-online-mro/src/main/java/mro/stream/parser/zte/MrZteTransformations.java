package mro.stream.parser.zte;

import mro.stream.source.zte.CollectZteFileFtp;
import mro.stream.source.zte.InitialZteData;
import mro.utils.KafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.apache.flink.util.Collector;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * @author : lijichen
 * @date :  2020/4/9  13:57
 * @description:
 */
public class MrZteTransformations {

    public static void main(String[] args) throws Exception {
        /**定义运行环境*/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**启动重启机制*/
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                10,
                Time.of(5, TimeUnit.MINUTES),
                Time.of(15, TimeUnit.SECONDS)
        ));
        /**初始化竞争资源*/
        InitialZteData.checkJedis();
        /**广播变量命名定义*/
        final MapStateDescriptor<String, String> CONFIG_KEYWORDS = new MapStateDescriptor<>(
                "config-keywords",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);

        /**自定义广播流，，实现读取Hdfs文件数据下载更新匹配数据*/
        BroadcastStream<Map<String, String>> broadcastStream = env.addSource(new ZteHdfsSource()).setParallelism(1).broadcast(CONFIG_KEYWORDS);

        DataStream<Map<String, String>> listdata = env.addSource(new CollectZteFileFtp()).setParallelism(48)
                .flatMap(new FlatMapFunction<byte[], Map<String, String>>() {
                    @Override
                    public void flatMap(byte[] bytes, Collector<Map<String, String>> collector) {
                        if (bytes.length > 10) {
                            collector.collect(toStr(bytes));
                        }
                    }
                }).setParallelism(204);
        /**filter过滤测试流*/
        SingleOutputStreamOperator<String> text = listdata.connect(broadcastStream)
                .process(new ZteProcessStream())
                .name("ZteProcess");

        text.print("Zte");

        /**sink to kafka*/
//        text.addSink(new FlinkKafkaProducer010<String>(
//                "mro_src_1",
//                new MySchema(),
//                KafkaUtil.evalKafkaConfigue(),
//                new MyPartitioner()
//        )).setParallelism(240);
        env.execute("ZX-20200901");
    }

    /**
     * 封装小文件流解压返回字符串
     */
    public static Map<String, String> toStr(byte[] str) {
        Map<String, String> map = new HashMap<>();
        try {
            InputStream inputStream = new ByteArrayInputStream(str);
            ZipInputStream zip = new ZipInputStream(inputStream);
            ZipEntry zipEntry = null;
            while ((zipEntry = zip.getNextEntry()) != null) {
                map.put("fileName", zipEntry.getName());
                System.out.println(zipEntry.getName());
                int len = 0;
                byte[] buf = new byte[2048];
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                while ((len = zip.read(buf)) > 0) {
                    baos.write(buf, 0, len);
                }
                byte[] dest = baos.toByteArray();
                System.out.println(new String(dest).length());
                map.put("fileString", new String(dest));
                return map;
            }
            zip.close();
        } catch (Exception e) {
            System.out.println("Unzipping error");
        }
        map.put("fileString", "LJC");
        return map;
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
