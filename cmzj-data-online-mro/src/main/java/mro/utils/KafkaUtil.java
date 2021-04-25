package mro.utils;

import java.util.Properties;

/**
 * @ClassName KafkaUtil
 * @Description //TODO
 * @Author 刘晓雨
 * @Date 2021/4/21 0:48
 * @Version 1.0
 **/
public class KafkaUtil {
    /*生产者配置*/
    public static Properties evalKafkaConfigue() {
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
        properties.setProperty("acks", "1");
        return properties;
    }
}
