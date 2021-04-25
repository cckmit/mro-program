package mro.stream.source.eric;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Set;

/**
 * @program: cmzj-data-online-mro
 * @description: 初始化服务器信息，用于后面的队列消费
 **/
public class InitialEricData {
    private static final String NAME = "Eric-Station";

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
            String str = jedis.lpop(NAME);
            if (str == null) {
                break;
            }
        }
        jedis.rpush("Eric-Station", "10.212.133.110;21;jituan;Jit$2018;/opt/ericsson/data/northbound/ready/");
        jedis.rpush("Eric-Station", "10.212.141.88;21;jituan;Jit$2018;/opt/ericsson/data/northbound/ready/");
        jedis.rpush("Eric-Station", "10.211.54.227;21;jituan;Jit$2018;/opt/ericsson/data/northbound/ready/");
        jedis.rpush("Eric-Station", "10.211.54.228;21;jituan;Jit$2018;/opt/ericsson/data/northbound/ready/");
    }

}
