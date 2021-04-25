package mro.stream.source.zte;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import mro.utils.RedisCluster;
import org.apache.commons.io.IOUtils;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import redis.clients.jedis.JedisCluster;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * @author : lijichen
 * @date :  2020/6/9
 * @description: 测试流程 - 文件下载
 */
public class CollectZteFileFtp extends RichParallelSourceFunction<byte[]> {
    private boolean flag = true;
    private static JedisCluster jedis = null;
    private long time_15 = System.currentTimeMillis();
    private static long time_interval = System.currentTimeMillis();

    private static void downFTPFile(List<String> filePathList, ChannelSftp channelSftp1, SourceContext<byte[]> src, List<String> logList) {
        int createFileCount = filePathList.size();
        //i为循环因子，遍整个list，j为每次剔除已下载文件后的下一个文件坐标，当成功删除，下一个对象下标前移，j不变，若下载失败，j+1
        for (int i = 0, j = 0; i < createFileCount; i++) {
            /**判断时间*/
            if (System.currentTimeMillis() - time_interval > 900000) {
                time_interval = System.currentTimeMillis();
                break;
            }
            InputStream bStream = null;
            ZipEntry zipEntry = null;
            try {
                byte[] tempBytes = IOUtils.toByteArray(channelSftp1.get(filePathList.get(j)));
                //效验是否有文件生成
                if (tempBytes.length > 100) {
                    bStream = new ByteArrayInputStream(tempBytes);
                    ZipInputStream zip = new ZipInputStream(new BufferedInputStream(bStream));
                    while ((zipEntry = zip.getNextEntry()) != null) {
                        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                            int len = 0;
                            byte[] buf = new byte[2048];
                            while ((len = zip.read(buf)) > 0) {
                                baos.write(buf, 0, len);
                            }
                            byte[] dest = baos.toByteArray();
                            baos.close();
                            if (dest.length > 10) {
                                src.collect(dest);
                            }
                            logList.add(zipEntry.getName());
                            Thread.sleep(500);
                        } catch (Exception e) {
                            System.out.println(e.toString());
                        }
                    }
                    zip.close();
                    //迁移成功后信息插入
                    logList.add(filePathList.get(j));
                    //目标文件数组信息西在下载成功的删除记录
                    filePathList.remove(j);
                }
            } catch (Exception e) {
                System.out.println("download error" + e.toString());
                j++;
            } finally {
                try {
                    if (bStream != null) {
                        bStream.close();
                    }
                } catch (Exception e) {
                    System.out.println(e.toString());
                }
            }
        }
    }


    @Override
    public void run(SourceContext<byte[]> sourceContext) throws Exception {
        /**测试保障程序不停*/
        List<String> logList = new ArrayList<>(1000000);
        /** 初始化TS服务器信息 */
        if (jedis == null) {
            jedis = RedisCluster.getJedis();
        }
        String Ts_information = CollectZteTools.getServeInfo(jedis);
        String[] Ts_str = Ts_information.split(";");
        String sftpHost = Ts_str[0];
        int sftpPort = Integer.parseInt(Ts_str[1]);
        String sftpUser = Ts_str[2];
        String sftpPass = Ts_str[3];
        String sftpScanPath = Ts_str[4];

        //sftp连接对象
        ChannelSftp channelSftp1 = null;
        Session session = null;
        Channel channel = null;

        //保存生成源文件的信息
        List<String> filePathList = new ArrayList<>();
        //保存迁移目标的文件信息
        Map<String, String> map = new HashMap<>();
        map.put("FDD-LTE_MRO_ZTE_OMC1_20200718234500.zip", "1");
        map.put("FDD-LTE_MRO_ZTE_OMC1_20200718234500_1.zip", "1");
        map.put("FDD-LTE_MRO_ZTE_OMC1_20200718234500_2.zip", "1");
        map.put("TD-LTE_MRO_ZTE_OMC1_20200718000000.zip", "1");
        map.put("TD-LTE_MRO_ZTE_OMC1_20200718000000_1.zip", "1");
        map.put("TD-LTE_MRO_ZTE_OMC1_20200718000000_2.zip", "1");
        while (flag) {
            try {
                /*重新获取SFTP连接对象，防止对象无效，每隔十五分钟重新获取一次*/
                if (System.currentTimeMillis() > time_15 + 1000 * 60 * 15) {
                    time_15 = System.currentTimeMillis();
                    if (channelSftp1 != null) {
                        channelSftp1.disconnect();
                    }
                    if (channel != null) {
                        channel.disconnect();
                    }
                    if (session != null) {
                        session.disconnect();
                    }
                    JSch jsch = new JSch();
                    session = jsch.getSession(sftpUser, sftpHost, sftpPort);
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
            Map<String, String> fileNameDate = CollectZteTools.createFileName();
            String dateStringa = fileNameDate.get("billDate");
            String dateStrings = fileNameDate.get("billDateHmsS").replaceAll("-", "");

            /**按照文件要求生成绝对路径信息的下载信息*/
            for (Map.Entry<String, String> entry : map.entrySet()) {
                if (entry.getKey().contains("FDD")) {
                    System.out.println(sftpScanPath + dateStringa + "/" + entry.getKey().replace(entry.getKey().substring(21, 35), dateStrings));
                    filePathList.add(sftpScanPath + dateStringa + "/" + entry.getKey().replace(entry.getKey().substring(21, 35), dateStrings));
                } else {
                    System.out.println(sftpScanPath + dateStringa + "/" + entry.getKey().replace(entry.getKey().substring(20, 34), dateStrings));
                    filePathList.add(sftpScanPath + dateStringa + "/" + entry.getKey().replace(entry.getKey().substring(20, 34), dateStrings));
                }
            }
            downFTPFile(filePathList, channelSftp1, sourceContext, logList);
            /**下载日志落入Hdfs*/
            if (logList.size() > 1) {
                CollectZteTools.toHdfs(logList, sftpHost);
                logList.clear();
            }
            /**保留两个小时，超出两个小时的数据直接抛出删除处理*/
            if (filePathList.size() > 4800) {
                filePathList = filePathList.subList(filePathList.size() - 600, filePathList.size());
            }
            Thread.sleep(1000 * 60 * 5);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
