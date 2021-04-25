package com.asiainfo.opmc.rtd.mro.utils;

import com.jcraft.jsch.*;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

/**
 * @program: cmzj-opmc-rtd-parent
 * @description: Sftp工具类
 * @author: sd
 * @create: 2021-01-28 15:45
 **/
@Slf4j
public class SftpUtil {

    public static ChannelSftp connect(String username, String password, String hostname, int port) {
        ChannelSftp sftp = null;
        try {
            JSch jsch = new JSch();
            jsch.getSession(username, hostname, port);
            Session sshSession = jsch.getSession(username, hostname, port);
            log.info("Session created ... UserName=" + username + ";host=" + hostname + ";port=" + port);
            sshSession.setPassword(password);
            Properties sshConfig = new Properties();
            sshConfig.put("StrictHostKeyChecking", "no");
            sshSession.setConfig(sshConfig);
            sshSession.connect();
            Channel channel = sshSession.openChannel("sftp");
            channel.connect();
            sftp = (ChannelSftp) channel;
            log.info("登录成功");
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        return sftp;
    }

    public static void disConnect(ChannelSftp sftp) {

        try {
            Session session = sftp.getSession();
            if (sftp != null) {
                sftp.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<String> listFiles(String directory, ChannelSftp sftp) throws SftpException {
        List<String> fileNameList = new ArrayList<>();
        try {
            sftp.cd(directory);
        } catch (SftpException e) {
            throw e;
        }
        Vector vector = sftp.ls(directory);
        for (Object o : vector) {
            if (o instanceof ChannelSftp.LsEntry) {
                ChannelSftp.LsEntry lsEntry = (ChannelSftp.LsEntry) o;
                String fileName = lsEntry.getFilename();
                if (fileName.contains("MRO") && !fileName.contains("_001.xml.gz")) {
                    fileNameList.add(fileName);
                }
            }
        }
        disConnect(sftp);
        return fileNameList;
    }

    public static List<String> listFilesDelay(String sourceIp, String directory, ChannelSftp sftp) throws SftpException {
        List<String> fileNameList = new ArrayList<>();
        try {
            sftp.cd(directory);
        } catch (SftpException e) {
            throw e;
        }
        Vector vector = sftp.ls(directory);
        for (Object o : vector) {
            if (o instanceof ChannelSftp.LsEntry) {
                ChannelSftp.LsEntry lsEntry = (ChannelSftp.LsEntry) o;
                String fileName = lsEntry.getFilename();
                fileNameList.add(fileName);
            }
        }
        disConnect(sftp);
        return fileNameList;
    }
}
