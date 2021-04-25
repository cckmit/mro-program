package mro.stream.parser.zte;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName ZteHdfsSource
 * @Description //TODO
 * @Author 刘晓雨
 * @Date 2021/4/20 23:14
 * @Version 1.0
 **/
public class ZteHdfsSource extends RichSourceFunction<Map<String, String>> {
    private volatile boolean isRunning = true;
    private final Map<String, String> maps = new HashMap<>(1000000);
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd 08:00:00");

    @Override
    public void run(SourceContext<Map<String, String>> sourceContext) {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        while (isRunning) {
            /**定时读取Hdfs文件*/
            URLConnection connection = null;
            InputStream inputStream = null;
            BufferedReader reader = null;
            try {
                URL url = new URL("hdfs://nsfed/ns2/jc_dwfu/dwfu_ns2_hive_db/i_cdm_enodeb/000000_0");
                connection = url.openConnection();
                inputStream = connection.getInputStream();
                reader = new BufferedReader(new InputStreamReader(inputStream));
                String line;
                while ((line = reader.readLine()) != null) {
                    maps.put(line.split("\t")[0].trim(), line.split("\t")[1]);
                }
                sourceContext.collect(maps);
            } catch (Exception e) {
                System.out.println("Read hdfs is err" + e.toString());
            } finally {
                try {
                    if (reader != null) {
                        reader.close();
                    }
                    if (inputStream != null) {
                        inputStream.close();
                    }
                    if (connection != null) {
                        connection.connect();
                    }
                } catch (Exception e) {
                    System.out.println("Read hdfs is err, Info:" + e.toString());
                }
            }
            /**每天早上8点更新一次数据*/
            try {
                Calendar cal = Calendar.getInstance();
                cal.add(Calendar.DATE, 1);
                Date time = cal.getTime();
                long scalarData = dateToStamp(sdf.format(time));
                Thread.sleep(scalarData - System.currentTimeMillis());
            } catch (Exception e) {
            }
        }
    }

    /**
     * 将指定日期转换成时间戳
     */
    public Long dateToStamp(String s) throws Exception {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(s);
        return date.getTime();
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
