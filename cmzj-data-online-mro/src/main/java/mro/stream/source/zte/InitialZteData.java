package mro.stream.source.zte;
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
public class InitialZteData {
    private static final String NAME = "ZTE53";

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
                String str = jedis.lpop(NAME);
                if (str == null) {
                    break;
                }
            } catch (Exception e) {
                System.out.println("CheckJedis初始化失败，异常信息：" + e.toString());
            }
        }
        jedis.rpush("ZTE53", "10.212.120.193;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.120.194;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.120.195;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.120.196;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.120.197;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.120.198;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.133.100;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.133.101;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.141.62;22;ltemr;MR$hz2019;/NDS/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.141.63;22;ltemr;MR$hz2019;/NDS/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.141.64;22;ltemr;MR$hz2019;/NDS/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.141.65;22;ltemr;MR$hz2019;/NDS/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.141.68;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.141.69;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.141.70;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.141.71;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.141.72;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.141.73;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.141.75;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.141.76;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.141.77;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.141.78;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.141.79;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.141.80;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.202.135;22;ltemr;MR$hz2019;/CDTReceiver/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.202.136;22;ltemr;MR$hz2019;/CDTReceiver/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.252.33;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.252.34;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.252.35;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.252.36;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.252.37;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.252.38;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.254.137;22;ltemr;MR$hz2019;/oracledata/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.76.95.161;22;ltemr;MR$hz2019;/oracledata/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.76.95.164;22;ltemr;MR$hz2019;/oracledata1/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.133.113;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.133.114;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.133.115;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.133.116;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.133.117;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.133.118;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.133.119;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.133.18;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.133.19;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.133.20;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.133.21;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.133.22;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
        jedis.rpush("ZTE53", "10.212.133.23;22;ltemr;MR$hz2019;/NDS-L/NDS-LN/output/north_output/NORTHFILE/MR/MRO/");
    }

}
