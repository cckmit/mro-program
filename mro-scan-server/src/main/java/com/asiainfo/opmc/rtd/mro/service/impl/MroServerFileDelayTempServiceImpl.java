package com.asiainfo.opmc.rtd.mro.service.impl;


import com.asiainfo.opmc.rtd.mro.entity.po.MroServerFileDelayPo;
import com.asiainfo.opmc.rtd.mro.entity.po.ServerInfo;
import com.asiainfo.opmc.rtd.mro.mapper.MroServerFileDelayTempMapper;
import com.asiainfo.opmc.rtd.mro.service.MroServerFileDelayTempService;
import com.asiainfo.opmc.rtd.mro.utils.SftpUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.jcraft.jsch.ChannelSftp;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Vector;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author shendan
 * @since 2021-03-23
 */
@Service
@Slf4j
public class MroServerFileDelayTempServiceImpl extends ServiceImpl<MroServerFileDelayTempMapper, MroServerFileDelayPo> implements MroServerFileDelayTempService {

    @Override
    public void computeFileDelay(String scanFile, String timeFormat) {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        String opTime = DateTime.now().toString(timeFormat);
//        ArrayList<String> opTimes = new ArrayList<>();
//        opTimes.add("20210318");
//        opTimes.add("20210319");
        ArrayList<ServerInfo> fileDelayServerInfo = new ArrayList<>();
        try {
            fileDelayServerInfo = this.getFileDelayServerInfo(scanFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (ServerInfo serverInfo : fileDelayServerInfo) {
            ChannelSftp connect = SftpUtil.connect(serverInfo.getUserName(), serverInfo.getPassword(), serverInfo.getHostName(), serverInfo.getPort());
            Vector<ChannelSftp.LsEntry> dirs = new Vector<>();

//            for (int i = 0; i < opTimes.size(); i++) {
//                String opTime = opTimes.get(i);

            try {
                dirs = connect.ls(serverInfo.getBasePath() + opTime + "/");
            } catch (Exception e) {
                log.error("no dir");
            }
            for (ChannelSftp.LsEntry lsEntry : dirs) {
                Vector<ChannelSftp.LsEntry> files = new Vector<>();
                try {
                    files = connect.ls(serverInfo.getBasePath() + opTime + "/" + lsEntry.getFilename() + "/");
                } catch (Exception e) {
                    log.error("no file");
                }
                for (ChannelSftp.LsEntry file : files) {
                    try {
                        String fileName = file.getFilename();
                        if (fileName.contains("MRO") && !fileName.contains("_001.xml.gz")) {
                            String timeStr = fileName.substring(fileName.length() - 21, fileName.length() - 7);
                            int delay = file.getAttrs().getMTime() - Integer.parseInt(String.valueOf(format.parse(timeStr).getTime() / 1000));
                            MroServerFileDelayPo mroServerFileDelayPo = new MroServerFileDelayPo(serverInfo.getHostName(), fileName, file.getAttrs().getMTime(), delay / 60);
                            this.save(mroServerFileDelayPo);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

//            }

        }
    }

    @Override
    public void scanFiles(String scanFile, String timeFormat) {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        String opTime = DateTime.now().toString(timeFormat);
        ArrayList<ServerInfo> fileDelayServerInfo = new ArrayList<>();
        try {
            fileDelayServerInfo = this.getFileDelayServerInfo(scanFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (ServerInfo serverInfo : fileDelayServerInfo) {
            ChannelSftp connect = SftpUtil.connect(serverInfo.getUserName(), serverInfo.getPassword(), serverInfo.getHostName(), serverInfo.getPort());
            Vector<ChannelSftp.LsEntry> dirs = new Vector<>();
            try {
                dirs = connect.ls(serverInfo.getBasePath() + opTime + "/");
            } catch (Exception e) {
                log.error("no dir");
            }
            for (ChannelSftp.LsEntry lsEntry : dirs) {
                Vector<ChannelSftp.LsEntry> files = new Vector<>();
                try {
                    log.info(serverInfo.getBasePath() + opTime + "/" + lsEntry.getFilename() + "/");
                    files = connect.ls(serverInfo.getBasePath() + opTime + "/" + lsEntry.getFilename() + "/");
                } catch (Exception e) {
                    log.error("no file");
                }
                for (ChannelSftp.LsEntry file : files) {
                    try {
                        String fileName = file.getFilename();
                        if (fileName.contains("MRO")) {
                            MroServerFileDelayPo mroServerFileDelayPo = new MroServerFileDelayPo(serverInfo.getHostName(), fileName, null, null);
                            this.save(mroServerFileDelayPo);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public ArrayList<ServerInfo> getFileDelayServerInfo(String fileName) throws IOException {
        ArrayList<ServerInfo> serverInfos = new ArrayList<>();
        //读取文件信息
        Resource resource = new ClassPathResource("serverinfo/" + fileName);
        InputStreamReader in = new InputStreamReader(resource.getInputStream(), "UTF-8");
        BufferedReader br = new BufferedReader(in);
        String s = "";
        while ((s = br.readLine()) != null) {
            String[] s1 = s.split("\t");
            serverInfos.add(new ServerInfo(s1[0], Integer.parseInt(s1[1]), s1[2], s1[3], s1[4]));
        }
        return serverInfos;
    }

//    public static void main(String[] args) throws ParseException {
//        String fileName = "FDD-LTE_MRO_HUAWEI_172031030135_460616_20210323000000.xml.gz";
//        String timeStr = fileName.substring(fileName.length() - 21, fileName.length() - 7);
//        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
//        int time = (int) format.parse(timeStr).getTime();
//        int delay = 1616431273 - Integer.parseInt(String.valueOf(format.parse(timeStr).getTime() / 1000));
//        System.out.println(delay / 60);
//        System.out.println("time" + time);
//        System.out.println("delay" + delay);
//    }
}
