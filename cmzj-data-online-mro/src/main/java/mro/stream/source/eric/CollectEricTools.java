package mro.stream.source.eric;

import mro.utils.FtpUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import redis.clients.jedis.JedisCluster;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @ClassName TransMroTools
 * @Description //TODO
 * @Author 刘晓雨
 * @Date 2021/4/19 19:46
 * @Version 1.0
 **/
public class CollectEricTools {
    private static final String NAME = "Eric-Station";

    /**
     * 生成文件时间，和拼接文件的时间戳
     */
    public static Map<String, String> createFileName() {
        String strMinutes = null;
        String strHours = null;
        Date date = new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        /**获取时间对象用于获取当前时间，包括日期，小时和分钟*/
        int hours = calendar.get(Calendar.HOUR_OF_DAY);
        int minutes = calendar.get(Calendar.MINUTE);
        Map<String, String> reMap = new HashMap<>(2);
        // yyyyMMdd
        String billDate = null;
        // 20200421143000
        String billDateHmsS = null;
        /**该条件判定主要是为了区分是不是00:00*/
        if (hours > 0 || minutes > 14) {
            /**生成45分钟到60分钟之间的数据*/
            if (minutes < 15 || minutes == 59) {
                strMinutes = "45";
                if (hours < 11) {
                    strHours = "0" + (hours - 1) + "";
                } else {
                    strHours = (hours - 1) + "";
                }
                date = calendar.getTime();

                SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                billDate = format.format(date);
                billDateHmsS = format.format(date) + strHours + strMinutes + "00";
            }
            /**生成0分钟到15分钟之间的数据*/
            if (minutes >= 15 && minutes < 30) {
                strMinutes = "00";
                if (hours < 10) {
                    strHours = "0" + hours + "";
                } else {
                    strHours = hours + "";
                }
                date = calendar.getTime();
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                billDate = format.format(date);
                billDateHmsS = format.format(date) + strHours + strMinutes + "00";
            }
            /**生成15分钟到30分钟之间的数据*/
            if (minutes >= 30 && minutes < 45) {
                strMinutes = "15";
                if (hours < 10) {
                    strHours = "0" + hours + "";
                } else {
                    strHours = hours + "";
                }
                date = calendar.getTime();
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                billDate = format.format(date);
                billDateHmsS = format.format(date) + strHours + strMinutes + "00";
            }
            /**生成30分钟到45分钟之间的数据*/
            if (minutes >= 45 && minutes < 59) {
                strMinutes = "30";
                if (hours < 10) {
                    strHours = "0" + hours + "";
                } else {
                    strHours = hours + "";
                }
                date = calendar.getTime();
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                billDate = format.format(date);
                billDateHmsS = format.format(date) + strHours + strMinutes + "00";
            }
        } else {
            /**生成夜晚23:45分钟到24:00分钟之间的数据*/
            calendar.add(Calendar.DATE, -1);
            strMinutes = "45";
            strHours = 23 + "";
            date = calendar.getTime();
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
            billDate = format.format(date);
            billDateHmsS = format.format(date) + strHours + strMinutes + "00";
        }
        reMap.put("billDate", billDate);
        reMap.put("billDateHmsS", billDateHmsS);
        return reMap;
    }

    /**
     * @Author 刘晓雨
     * @Description //
     * @Date 20:49 2021/4/19
     * @Param [conn, ip, map, data]
     * @return [java.sql.Connection, java.lang.String, java.util.Map<java.lang.String,java.lang.String>, java.lang.String]
     **/
    public static Map<String, String> extract(Connection conn, String ip, Map<String, String> map, String data) {
        try {
            PreparedStatement stmt = conn.prepareStatement("SELECT distinct ts_format FROM mro_scan_station_list where ts_ip='" + ip + "' and  OP_TIME ='" + data + "'");//LTE_MRO_HUAWEI_172031001053_764764_20200717000000.xml.gz(前置程序扫描出来的)
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                map.put(rs.getString("ts_format"), "1");  //?这个字段的含义是什么？
            }
        } catch (Exception e) {
            System.out.println("根据ip和账期从Mysql获取文件绝对路径时，连接Mysql异常" + e.toString());
        }
        return map;
    }

    /**
     * 更新获取ts服务器信息
     * 资源竞争
     */
    public static String getServeInfo(JedisCluster jedis) {
        String sftpHost = null;
        int sftpPort = 22;
        String sftpUser = null;
        String sftpPass = null;
        String sftpScanPath = null;
        String Ts_information = "";

        while (true) {
            /*睡眠，避开重复消费*/
            try {
                while (true) {
                    /*找出队列中key为name的value ,队列*/
                    Ts_information = jedis.lpop(NAME);
                    if (Ts_information.length() > 10) {
                        break;
                    }
                }
                System.out.println("Access to the--->" + Ts_information);
                String[] strs = Ts_information.split(";");
                if (strs.length > 4) {
                    sftpHost = strs[0];
                    sftpPort = Integer.parseInt(strs[1]);
                    sftpUser = strs[2];
                    sftpPass = strs[3];
                    sftpScanPath = strs[4];
                }
                /*验证,涉及事务，有可能同时插入，插入失败的原因有很多，有可能事务没提交，需要在提交插入*/
                if (new FtpUtil(false).login(sftpHost, sftpPort, sftpUser, sftpPass)) {
                    Ts_information = sftpHost + ";" + sftpPort + ";" + sftpUser + ";" + sftpPass + ";" + sftpScanPath;
                    System.out.println("Log in successfully--->" + Ts_information);
                    break;
                } else {
                    /*登录不了直接回填,插入redis队列中*/
                    if (Ts_information.length() > 22) {
                        jedis.rpush(NAME, Ts_information);
                    }
                }
            } catch (Exception e) {
                System.out.println("failure ip-->" + sftpHost + "," + e.getMessage());
            }
        }
        return Ts_information;
    }

    /**
     * 将下载成功的文件信息添加到hdfs的.txt文件中
     */
    public static void toHdfs(List<String> list, String sftpHost) {//下载成功的文件信息(包括文件），Ip
        try {
            Calendar cal = Calendar.getInstance();
            Date time = cal.getTime();
            String data = new SimpleDateFormat("yyyyMMddHH").format(time);

            String hdfs_path = "hdfs://nsfed/ns2/c_zz_mro/mro_ns2_hive_db/i_cdm_enodeb/" + data + "/" + sftpHost + ".txt";  //+"_"+new SimpleDateFormat("yyyyMMddHH").format(time)
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            FileSystem fs = null;
            conf.setBoolean("dfs.support.append", true);
            //获得hadoop系统的连接，hdfs连接
            fs = FileSystem.get(URI.create(hdfs_path), conf);
            if (fs.exists(new Path(hdfs_path))) {  //文件是否存在
                /**文件存在追加文件*/
                //out对应的是Hadoop文件系统中的目录
                OutputStream output = fs.append(new Path(hdfs_path));
                //4096是4k字节    将本地文件上传至hdfs
                org.apache.hadoop.io.IOUtils.copyBytes(new ByteArrayInputStream(list.toString().getBytes()), output, 4096, true);
                fs.close();
                output.close();

            } else {
                /**文件不存在，创建文件*/
                //out对应的是Hadoop文件系统中的目录
                OutputStream output = fs.create(new Path(hdfs_path));
                //4096是4k字节
                org.apache.hadoop.io.IOUtils.copyBytes(new ByteArrayInputStream(list.toString().getBytes()), output, 4096, true);
                fs.close();
                output.close();

            }
        } catch (Exception e) {
            System.out.println("Failed to upload file to HDFS," + e.toString());
        }
    }

}
