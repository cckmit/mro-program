package mro.stream.source.nokia;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Set;

/**
 * @program: cmzj-data-online-mro
 * @description: 初始化服务器信息，用于后面的队列消费
 * @author: Mr.Li
 * @create: 2020-07-13 09:48
 **/
public class InitialNokiaData {
    private static final String NAME = "NOKIA";   //队列名称NOKIA

    public static void checkJedis() {
        JedisPoolConfig jpc = new JedisPoolConfig();
        Set<HostAndPort> nodes = new HashSet<>();
        jpc.setMaxTotal(2); //设置最大实例总数
        jpc.setMinIdle(100);
        jpc.setMaxIdle(100); //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例
        jpc.setMaxWaitMillis(10000);
        jpc.setTestOnBorrow(true);
        jpc.setTestOnReturn(true);
        nodes.add(new HostAndPort("10.76.188.187", 6201));
        nodes.add(new HostAndPort("10.76.245.183", 6201));
        nodes.add(new HostAndPort("10.76.188.188", 6202));
        nodes.add(new HostAndPort("10.76.245.184", 6202));
        nodes.add(new HostAndPort("10.76.188.189", 6203));
        nodes.add(new HostAndPort("10.76.245.185", 6203));
        JedisCluster jedis = new JedisCluster(nodes, 10000, 10000, 100, "cmVkaXM=", jpc);
        while (true) {
            try {
                String str = jedis.lpop(NAME);  //返回为NOKIA的基站
                if (str == null) {
                    break;
                }
            } catch (Exception e) {
                System.out.println("CheckJedis初始化失败，异常信息：" + e.toString());
            }
        }
        jedis.rpush("NOKIA", "10.212.120.24;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.120.25;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.120.26;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.120.27;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.120.28;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.120.29;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.120.30;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.120.31;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.120.32;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.120.33;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.120.35;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.120.36;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.120.37;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.120.38;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.120.39;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.120.40;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.120.41;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.120.42;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.120.43;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.124.216;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.124.217;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.124.218;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.124.219;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.124.220;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.124.224;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.124.225;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.124.226;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.124.227;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.124.228;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.139.10;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.139.11;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.139.12;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.139.13;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.139.14;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.139.15;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.139.16;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.139.17;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.139.18;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.139.19;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.139.20;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.139.5;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.139.6;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.139.7;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.139.8;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.139.9;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.141.43;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.10;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.11;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.12;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.13;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.14;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.15;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.16;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.17;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.18;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.19;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.20;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.21;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.22;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.23;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.24;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.25;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.26;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.27;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.28;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.29;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.4;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.5;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.6;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.7;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.8;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.142.9;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.247.225;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.247.226;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.247.227;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.247.228;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.247.229;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.247.230;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.247.231;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.247.232;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.247.233;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.247.242;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.247.243;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.247.244;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.254.251;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.254.252;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.254.253;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.254.254;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
        jedis.rpush("NOKIA", "10.212.254.255;22;richuser;Tsbyj#12l5c;/home/richuser/l3fw_mr/kpi_import/");
    }
}
