package mro.stream.source.dt;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Set;

/**
 * @program: cmzj-data-online-mro
 * @description: 初始化服务器信息，用于后面的队列消费
 **/
public class InitialDtData {
    private static final String NAME = "DT-Station";

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
        jedis.rpush("DT-Station", "10.78.89.158;21;omcrftp;Lion$2019;/export/home/mrftp/mrfile/dtmrfile/");
        jedis.rpush("DT-Station", "10.78.89.186;21;omcrftp;Lion$2019;/home/mrftp/mrfile/dtmrfile/");
    }
}
