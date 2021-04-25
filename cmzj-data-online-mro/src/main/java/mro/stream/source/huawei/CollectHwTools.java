package mro.stream.source.huawei;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import redis.clients.jedis.JedisCluster;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @ClassName CollectHWTools
 * @Description //TODO
 * @Author 刘晓雨
 * @Date 2021/4/20 9:37
 * @Version 1.0
 **/
public class CollectHwTools {
    private static final String NAME = "station-huawei";

    public static int evalSleepTime(ArrayList<String> fileList) {
        int sm = 0;
        /*按照文件基站数不同，进行不同的睡眠时间*/
        if (fileList.size() > 500) {
            sm = 100;
        } else if (fileList.size() > 400) {
            sm = 500;
        } else if (fileList.size() > 350) {
            sm = 700;
        } else if (fileList.size() > 300) {
            sm = 800;
        } else if (fileList.size() > 250) {
            sm = 1000;
        } else if (fileList.size() > 200) {
            sm = 1500;
        } else if (fileList.size() > 150) {
            sm = 2000;
        } else if (fileList.size() > 100) {
            sm = 3000;
        } else if (fileList.size() > 50) {
            sm = 4000;
        } else {
            sm = 5000;
        }
        return sm;
    }

    /**
     * 连接SFTP对象获取，返回连接对象,测试网络用于调换基站信息
     */
    public static Boolean sftpConnect(String username, String password, int port, String host) throws JSchException {
        try {
            JSch jsch = new JSch();
            Session session = jsch.getSession(username, host, port);
            session.setPassword(password);
            Properties sshConfig = new Properties();
            sshConfig.put("StrictHostKeyChecking", "no");
            session.setConfig(sshConfig);
            session.connect();
            Channel channel = session.openChannel("sftp");
            channel.connect();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            try {
                InetAddress addr = InetAddress.getLocalHost();
                System.out.println("Local HostAddress:" + addr.getHostAddress() + ";UserName=" + username + ";host=" + host + ";password=" + password);
            } catch (UnknownHostException ex) {
                System.out.println("无法获取sftp的连接的时候，错误信息为：" + e.toString());
            }
            return false;
        }
        return true;
    }


    /**
     * 更新获取ts服务器信息
     * 资源竞争
     */
    public static String getServeInfo(JedisCluster jedis) {
        String sftpHost = "";
        int sftpPort = 22;
        String sftpUser = "";
        String sftpPass = "";
        String tsInfo = "";
        /**查看连接对象是否还存活*/

        while (true) {
            /**睡眠，避开重复消费*/
            try {
                do {
                    tsInfo = jedis.lpop(NAME);  //找出队列中key为name的value ,队列
                } while (tsInfo.length() <= 10);
                System.out.println("Access to the--->" + tsInfo);
                String[] strs = tsInfo.split(";");
                if (strs.length > 4) {
                    sftpHost = strs[0];
                    sftpPort = Integer.parseInt(strs[1]);
                    sftpUser = strs[2];
                    sftpPass = strs[3];
                }
                //进行能否连接ftp验证，成功就插入数据表中
                if (sftpConnect(sftpUser, sftpPass, sftpPort, sftpHost)) {     //涉及事务，有可能同时插入，插入失败的原因有很多，有可能事务没提交，需要在提交插入
                    tsInfo = sftpHost + ";" + sftpPort + ";" + sftpUser + ";" + sftpPass + ";";
                    System.out.println("Log in successfully--->" + tsInfo);
                    break;
                } else {
                    /**登录不了直接回填*/
                    jedis.rpush(NAME, tsInfo);  //插入redis队列中
                    System.out.println("can't login------>" + tsInfo);
                }
                Thread.sleep((int) (Math.random() * 20000 + 1));
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }
        return tsInfo;
    }


    /**
     * 生成文件时间，和拼接文件的时间戳
     */
    public static Map<String, String> createFileName() {
        String strMinutes = null;
        String strHours = null;
        // yyyyMMdd
        String billDate = null;
        // 20200421143000
        String billDateHmsS = null;
        /**获取时间对象用于获取当前时间，包括日期，小时和分钟*/
        Date date = new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int hours = calendar.get(Calendar.HOUR_OF_DAY);
        int minutes = calendar.get(Calendar.MINUTE);
        Map<String, String> reMap = new HashMap<>(2);

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

    public static ArrayList<String> extract(Connection conn, String ip, String data) {
        ArrayList<String> strings = new ArrayList<>();
        try {
            PreparedStatement stmt = conn.prepareStatement("SELECT distinct ts_format FROM mro_scan_station_list where ts_ip='" + ip + "' and  OP_TIME ='" + data + "'");//LTE_MRO_HUAWEI_172031001053_764764_20200717000000.xml.gz(前置程序扫描出来的)
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                strings.add(rs.getString("ts_format"));
            }
        } catch (Exception e) {
            System.out.println("get fileName error :" + e.toString());
        }
        return strings;
    }

    /**
     * to Hdfs
     */   //将下载成功的文件信息添加到hdfs的.txt文件中
    public static void toHdfs(List<String> list, String sftpHost) {//下载成功的文件信息(包括文件），Ip
        try {
            Calendar cal = Calendar.getInstance();
            Date time = cal.getTime();
            String data = new SimpleDateFormat("yyyyMMddHH").format(time);

            String hdfs_path = "hdfs://nsfed/ns2/jc_zz_mro/mro_ns2_hive_db/i_cdm_enodeb/" + data + "/" + sftpHost + ".txt";  //+"_"+new SimpleDateFormat("yyyyMMddHH").format(time)
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
            System.out.println(e.toString());
            System.out.println("Failed to upload file to HDFS");
        }
    }
}
