package mro.service;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author : zhangbinbin
 * @date :  2020/4/9  14:13
 * @description: lijichen
 */
public class FileMoveService {

    public FileMoveService(){}

    /**
     * 连接sftp服务器
     * @throws Exception
     */
    public static Session login(String username, String password, String host, int port)throws Exception{

        JSch jsch = new JSch();
        Session session = jsch.getSession(username, host, port);
        if (password != null) {
            session.setPassword(password);
        }
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);
        session.connect();
        return session;

    }

    /**
     * 关闭连接 server
     */
    public void logout(ChannelSftp sftpobject, Session session){
        if (sftpobject != null) {
            if (sftpobject.isConnected()) {
                sftpobject.disconnect();
            }
        }
        if (session != null) {
            if (session.isConnected()) {
                session.disconnect();
            }
        }
    }


    /**
     * 将输入流的数据上传到sftp作为文件
     * @param sftpFileName sftp端文件名
     * @param input 输入流
     * @throws SftpException
     * @throws Exception
     */
    public static void upload( String sftpFileName, InputStream input,ChannelSftp sftpobject) throws SftpException {

        /**创建账期路径*/
        String names1=sftpFileName.substring(0,sftpFileName.lastIndexOf("/"));
        String names=names1.substring(0,names1.lastIndexOf("/"));
        try {
            sftpobject.cd(names);
        }catch (Exception e1){
            sftpobject.mkdir(names);
            System.out.println("name=="+names);
        }finally {
            sftpobject.cd("/");
            try {
                sftpobject.cd(sftpFileName.substring(0,sftpFileName.lastIndexOf("/")));
            } catch (SftpException e) {
                System.out.println("newname=="+sftpFileName.substring(0,sftpFileName.lastIndexOf("/")));
                sftpobject.mkdir(sftpFileName.substring(0,sftpFileName.lastIndexOf("/")));
                System.out.println("The_interception"+sftpFileName.substring(0,sftpFileName.lastIndexOf("/")));
                sftpobject.cd(sftpFileName.substring(0,sftpFileName.lastIndexOf("/")));
            }
            sftpobject.put(input, sftpFileName);
            try {
                if(input!=null){
                    input.close();
                    input=null;
                }
            }catch (Exception e){e.printStackTrace();}

        }
    }



    /**
     * 将byte[]上传到sftp，作为文件。注意:从String生成byte[]是，要指定字符集。
     * @param sftpFileName 文件在sftp端的命名
     * @param byteArr 要上传的字节数组
     * @throws SftpException
     * @throws Exception
     */
    public static void upload( String sftpFileName, byte[] byteArr,ChannelSftp sftpobject) throws SftpException{
        InputStream inputStream=new ByteArrayInputStream(byteArr);
        upload(sftpFileName, inputStream,sftpobject);
        try {
            byteArr=null;
            if(inputStream!=null){
                inputStream.close();
                inputStream=null;
            }
        }catch (Exception e){e.printStackTrace();}
    }



    /**
     * 下载文件
     * @param name 下载目录
    //     * @param downloadFile 下载的文件名
     * @return 字节数组
     * @throws SftpException
     * @throws IOException
     * @throws Exception
     */
    public  static  byte[] download(String name,ChannelSftp sftpobject) throws SftpException, IOException{
        InputStream is=null;
        try {
            is= sftpobject.get(name);
            byte[] fileData = IOUtils.toByteArray(is);
            return fileData;
        }catch (Exception e){

        }finally {
            if(is!=null){
                is.close();
            }

        }
        byte [] judge={1};
        return judge;
    }

}

