package mro.stream.source.nokia;


import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import mro.utils.DateUtil;
import mro.utils.RedisCluster;
import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import redis.clients.jedis.JedisCluster;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * @Description:
 * @Author: sd
 * @Date: 2021/2/23 9:42
 **/
public class CollectNokiaFileFtp extends RichParallelSourceFunction<Map<String, String>> {
    private boolean flag = true;
    private static JedisCluster jedis = null;
    private static final long serialVersionUID = 2174904787118597072L;
    private long time_15 = System.currentTimeMillis();
    private static long time_interval = System.currentTimeMillis();
    private Connection conn = null;
    private static final String DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://10.76.217.186:3306/mr_info?serverTimezone=UTC";
    private static final String USER = "mr_info";
    private static final String PASSWORD = "mr_info1q#";


    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Failed to create Mysql connection object A");
        }
        while(true){
            try{
                conn = DriverManager.getConnection(URL, USER, PASSWORD);
            }catch (Exception e){
                continue;
            }
            break;
        }
    }

    private void downFTPFile(List<String> filePathList, Map<String, String> mapstr, String Ts_information, ChannelSftp channelSftp1, SourceContext<Map<String, String>> src, List<String> logList) {
        String fileName = "";
        int createFileCount = filePathList.size();
        //i为循环因子，遍整个list，j为每次剔除已下载文件后的下一个文件坐标，当成功删除，下一个对象下标前移，j不变，若下载失败，j+1
        for (int i = 0, j = 0; i < createFileCount; i++) {
            fileName = filePathList.get(j);
            /**判断时间*/
            if (System.currentTimeMillis() - time_interval > 900000) {
                time_interval = System.currentTimeMillis();
                break;
            }
            if (System.currentTimeMillis() - DateUtil.toTimestamp(fileName.substring(fileName.length() - 21, fileName.length() - 7), "yyyyMMddHHmmss") > 7200000) {
                filePathList.remove(j);
                continue;
            }
            if (fileName.endsWith(".xml")) {
                fileName += ".gz";
            }
            InputStream bStream = null;
            GZIPInputStream is = null;
            ByteArrayOutputStream baos = null;
            try {
                byte[] tempBytes = IOUtils.toByteArray(channelSftp1.get(fileName));   //将流转化为byte数组
                bStream = new ByteArrayInputStream(tempBytes);//字节数组输入流在内存中创建一个字节数组缓冲区，从输入流读取的数据保存在该字节数组缓冲区中
                /**提示回收*/
                is = new GZIPInputStream(bStream);//用于解压缩,将流转化为字符串
                baos = new ByteArrayOutputStream();
                int len = 0;
                byte[] buf = new byte[4096];
                while ((len = is.read(buf)) > 0) {
                    baos.write(buf, 0, len);   // 将指定的字节写入此字节数组输出流。
                }
                mapstr.put("filePath", fileName);//绝对路径信息的下载信息
                mapstr.put("dataContent", baos.toString());//字符串
                mapstr.put("Ts", Ts_information);  //ts服务器信息
                src.collect(mapstr);
                /**提示回收*/
                logList.add(fileName);
                filePathList.remove(j);
                /**防止下载数太多，造成fgc不过来*/
                Thread.sleep(100);
            } catch (Exception e) {
                System.out.println("download error" + e.toString());
                j++;
            } finally {
                try {
                    if (baos != null) {
                        baos.close();
                    }
                    if (is != null) {
                        is.close();
                    }
                    if (bStream != null) {
                        bStream.close();
                    }
                } catch (Exception ex) {
                    System.out.println("CollectFileFtp流关闭异常：" + ex.toString());
                }
            }
        }
    }

    @Override
    public void run(SourceContext<Map<String, String>> sourceContext) throws Exception {
        /**测试保障程序不停*/
        List<String> filePathList = new ArrayList<>(6000);
        Map<String, String> mapstr = new HashMap<>(50);
        List<String> logList = new ArrayList<>(1000000);
        /**查看连接对象是否还存活*/
        if (jedis == null) {
            jedis = RedisCluster.getJedis();
        }
        /** 初始化TS服务器信息 */
        String Ts_information = CollectNokiaTools.getServeInfo(jedis);
        if (!Ts_information.equals("") && Ts_information.contains(";")) {
            String[] Ts_str = Ts_information.split(";");//切割得到ip,端口,用户名,密码
            String sftpHost = Ts_str[0];
            int sftpPort = Integer.parseInt(Ts_str[1]);
            String sftpUser = Ts_str[2];
            String sftpPass = Ts_str[3];

            //sftp连接对象
            ChannelSftp channelSftp1 = null;
            Session session = null;
            Channel channel = null;
            while (flag) {
                try {
                    /**为了防止数据库连接对象失效，每隔十五分钟后重新连接一次*/
                    if (System.currentTimeMillis() > time_15 + 1000 * 60 * 15) {
                        time_15 = System.currentTimeMillis();
                        if (conn != null) {
                            conn.close();
                        }
                        conn = DriverManager.getConnection(URL, USER, PASSWORD);
                        /**重新获取SFTP连接对象，防止对象无效，每隔十五分钟重新获取一次*/
                        if (channelSftp1 != null) {
                            channelSftp1.disconnect();
                        }
                        if (channel != null) {
                            channel.disconnect();
                        }
                        if (session != null) {
                            session.disconnect();
                        }
                        JSch jsch = new JSch();    //？连接sshd 服务器，使用端口转发，X11转发，文件传输等等
                        session = jsch.getSession(sftpUser, sftpHost, sftpPort);//登陆
                        session.setPassword(sftpPass);
                        Properties sshConfig = new Properties();
                        sshConfig.put("StrictHostKeyChecking", "no");
                        session.setConfig(sshConfig);
                        session.connect();
                        channel = session.openChannel("sftp");
                        channel.connect();
                        channelSftp1 = (ChannelSftp) channel;
                    }
                } catch (Exception e) {
                    System.out.println("Failed to create SFTP connection," + e.toString());
                }
                /** 生成文件名称账期 */
                Map<String, String> fileNameDate = CollectNokiaTools.createFileName();
                String dateStringa = fileNameDate.get("billDate"); // yyyyMMdd
                String dateStrings = fileNameDate.get("billDateHmsS");// 20200421143000
                /**获取自动生成目录资源，使用Map迭代将map数据保存到对应List数组用于后面数据生成*/
                Map<String, String> map = CollectNokiaTools.extract(conn, sftpHost, new HashMap<>(), dateStringa); //根据ip和账期从Mysql获取文件绝对路径
                map.forEach((k, v) -> {
                    if(k.contains("/")){
                        String[] strs = k.split("/");
                        k = k.replace(strs[strs.length - 3], dateStringa);
                        k = k.replace(k.substring(k.length() - 21, k.length() - 7), dateStrings);
                        filePathList.add(k);
                    }
                });
                downFTPFile(filePathList, mapstr, Ts_information, channelSftp1, sourceContext, logList);
                if (logList.size() > 1) {    //>1代表有日志插入map，下面  logList.add(filePathList.get(i));将map插入
                    CollectNokiaTools.toHdfs(logList, sftpHost);  // to hdfs  通过ip地址和data来创建一个文件地址，连接hadoop上面的hdfs，判断hdfs是否存在该文件，如果存在就上传至hdfs
                    logList.clear();
                }
            }
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
