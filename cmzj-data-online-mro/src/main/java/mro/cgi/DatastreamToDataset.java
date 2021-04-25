package mro.cgi;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @program: cmzjdataonlinekl
 * @description: DataStream转换成DataSet
 * @author: Mr.Li
 * @create: 2020-08-26 17:49
 **/
public class DatastreamToDataset {
    public static void main(String[] args) throws Exception {
        System.out.println("vertion----20210413_1");
        /**环境构建*/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**间隔重启机制*/
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                10,
                org.apache.flink.api.common.time.Time.of(5, TimeUnit.MINUTES),
                org.apache.flink.api.common.time.Time.of(15, TimeUnit.SECONDS)
        ));
        /**消费者配置*/
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "10.76.183.77:21007,10.76.183.78:21007,10.76.183.79:21007");
        props.setProperty("security.protocol", "SASL_PLAINTEXT");
        props.setProperty("kerberos.domain.name", "hadoop.HD_JHJCFX.COM");
        props.setProperty("sasl.kerberos.service.name", "kafka");
        props.setProperty("group.id", "78555522225");
        props.setProperty("buffer.memory", "10485760");
        props.setProperty("max.request.size", "10485760");
        props.setProperty("max.poll.records", "500");
        props.setProperty("max.partition.fetch.bytes", "41943040");
        /***生产者配置*/
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.76.183.77:21007,10.76.183.78:21007,10.76.183.79:21007");
        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
        properties.setProperty("kerberos.domain.name", "hadoop.HD_JHJCFX.COM");
        properties.setProperty("sasl.kerberos.service.name", "kafka");
        properties.setProperty("retries", "1000000000");
        properties.setProperty("request.timeout.ms", "1000000000");
        properties.setProperty("retry.backoff.ms", "20000");
        properties.setProperty("batch.size", "163840");
        properties.setProperty("buffer.memory", "3221225472");
        properties.setProperty("max.request.size", "41943040");
        properties.setProperty("linger.ms", "500");
        properties.setProperty("acks", "0");

        DataStream<String> datakafka = env.addSource(new FlinkKafkaConsumer011<>(
                "mro_src_1",
                new SimpleStringSchema(),
                props)
        ).setParallelism(512)
                .name("kafkasource");

        DataStream<Tuple2<String, String>> MR = datakafka
                .flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
                    String[] data_resolution;
                    String[] str;
                    Tuple2<String, String> tuple2;

                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, String>> collector) {
                        try {
                            str = s.trim().split(", ");
                            for (String S : str) {
                                data_resolution = S.trim().split(" ");
                                tuple2 = new Tuple2<>(data_resolution[4] + "," + data_resolution[11] + "," + data_resolution[12], S.trim());
                                collector.collect(tuple2);
                            }

                        } catch (Exception e) {
                        }

                    }
                })
                .setParallelism(512).disableChaining().name("flatmap");

        /**加载cgi数据*/
        DataStream<Path> cgi = env.addSource(new RichSourceFunction<Path>() {
            @Override
            public void run(SourceContext<Path> sourceContext) {
                /**定时深度遍历hdfs文件夹*/
                URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
                Long scalarData;
                while (true) {
                    try {
                        Calendar cal = Calendar.getInstance();
                        String data = new SimpleDateFormat("yyyyMMdd").format(cal.getTime());
                        String hdfs_path = "hdfs://nsfed/ns2/jc_zz_mro/mro_ns2_hive_db/i_cdm_enodeb/Cgi" + data;  //
                        cal.add(Calendar.DATE, 1);
                        Date time = cal.getTime();
                        scalarData = dateToStamp(new SimpleDateFormat("yyyy-MM-dd 08:00:00").format(time));
                        System.out.println(hdfs_path);
                        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
                        FileSystem fs = null;
                        conf.setBoolean("dfs.support.append", true);
                        //获得hadoop系统的连接
                        fs = FileSystem.get(URI.create(hdfs_path), conf);
                        List<Path> list = getFilesUnderFolder(fs, new Path(hdfs_path), "10.");
                        for (Path filePath : list) {
                            try {
                                sourceContext.collect(filePath);
                            } catch (Exception e) {
                                System.out.println("dow err");
                            }

                        }
                    } catch (Exception e) {
                        System.out.println("err");
                        continue;
                    }

                    try {
                        Thread.sleep(scalarData - System.currentTimeMillis());
                    } catch (Exception e) {
                    }
                }

            }

            @Override
            public void cancel() {
            }

            /** 将指定日期转换成时间戳 */
            public Long dateToStamp(String s) throws Exception {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date date = simpleDateFormat.parse(s);
                return date.getTime();
            }

            public List<Path> getFilesUnderFolder(FileSystem fs, Path folderPath, String pattern) throws IOException {
                List<Path> paths = new ArrayList<Path>();
                if (fs.exists(folderPath)) {
                    FileStatus[] fileStatus = fs.listStatus(folderPath);
                    for (int i = 0; i < fileStatus.length; i++) {
                        FileStatus fileStatu = fileStatus[i];
                        if (!fileStatu.isDir()) {
                            try {
                                if (!fileStatu.getPath().toString().contains("SUCCESS")) {
                                    System.out.println(fileStatu.getPath().toString());
                                    paths.add(fileStatu.getPath());
                                }
                            } catch (Exception e) {
                                System.out.println("scan err");
                            }
                        }
                    }
                }
                return paths;
            }
        })
                .setParallelism(1)
//                .slotSharingGroup("group_1")
                .disableChaining()
                .name("加载HDFS_cgi");


        DataStream<Tuple2<String, String>> cgiData = cgi
                .flatMap(new FlatMapFunction<Path, String>() {
                    @Override
                    public void flatMap(Path path, Collector<String> collector) throws Exception {
                        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
                        FileSystem fs = null;
                        conf.setBoolean("dfs.support.append", true);
                        fs = FileSystem.get(URI.create(path.toString()), conf);
                        try {
                            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
                            String line;
                            while ((line = reader.readLine()) != null) {
                                try {
                                    collector.collect(line.trim());
                                } catch (Exception e) {
                                }
                            }
                            reader.close();
                        } catch (Exception e) {
                            System.out.println("dow err");
                        }
                    }
                })
                .setParallelism(24)
//                .slotSharingGroup("group_1")
                .flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
                    String[] strings = new String[4];

                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, String>> collector) throws Exception {
                        try {
                            strings = line.split(",");
                            collector.collect(new Tuple2<String, String>(strings[0].replace("(", "") + "," + strings[1] + "," + strings[2], strings[3] + " " + strings[4] + " " + strings[5].replace(")", "")));
                        } catch (Exception e) {
                        }
                    }
                })
                .setParallelism(24)
//                .slotSharingGroup("group_1")
                .disableChaining()
                .name("cgi_flatMap");

        OutputTag<String> outputTag = new OutputTag<String>("topic1") {
        };

        SingleOutputStreamOperator<String> process = MR
                .connect(cgiData)
                .keyBy(X -> X.f0, Y -> Y.f0)
                .process(new CoProcessFunction<Tuple2<String, String>, Tuple2<String, String>, String>() {
                    Map<String, String> map = new HashMap<>(2000000);
                    String str = null;
                    int subtask;
                    Boolean flag = true;
                    Boolean send = true;

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        send = true;
                        ctx.timerService().registerProcessingTimeTimer(timestamp + 30 * 1000);
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        subtask = getRuntimeContext().getIndexOfThisSubtask();
                    }

                    @Override
                    public void processElement1(Tuple2<String, String> tuple2, Context context, Collector<String> collector) {
                        if (subtask == 0) {
                            if (send) {
                                context.output(outputTag, tuple2.f1);
                                send = false;
                            }

                            if (flag) {
                                context.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 30 * 1000);
                                flag = false;
                            }
                        }

                        str = map.get(tuple2.f0);
                        if (str != null) {
                            collector.collect(tuple2.f1 + " " + str + " " + System.currentTimeMillis());
                        } else {
                            collector.collect(tuple2.f1 + " null null null " + System.currentTimeMillis());
                        }

                    }

                    @Override
                    public void processElement2(Tuple2<String, String> stringStringTuple2, Context context, Collector<String> collector) {
                        map.put(stringStringTuple2.f0, stringStringTuple2.f1);
                    }
                })
                .disableChaining().name("process");

        process.getSideOutput(outputTag)
                .addSink(new Sink2Mysql())
                .setParallelism(1)
                .disableChaining()
                .name("抽样入mysql");

        process.addSink(new FlinkKafkaProducer011<>(
                "mro_cgi_1",
                new SimpleStringSchema(),
                properties)
        ).setParallelism(500).name("sink-mro_cgi_1");

        env.execute("Flink-Cgi");
    }

}
