package mro.utils;

//import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author : zhangbinbin
 * @date :  2020/3/28  13:27
 * @description:
 */
//@Slf4j
public class FtpUtil {
    private static final int BUFFER_SIZE = 2048;
    private static String destDir = "D:\\tmp\\";
    public FTPClient ftp;
    public ArrayList<String> arFiles;

    /**
     * 重载构造函数
     *
     * @param isPrintCommmand 是否打印与FTPServer的交互命令
     */
    public FtpUtil(boolean isPrintCommmand) {
        ftp = new FTPClient();
        arFiles = new ArrayList<String>();
        if (isPrintCommmand) {
            ftp.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out)));
        }
    }

    /**
     * 登陆FTP服务器
     *
     * @param host     FTPServer IP地址
     * @param port     FTPServer 端口
     * @param username FTPServer 登陆用户名
     * @param password FTPServer 登陆密码
     * @return 是否登录成功
     * @throws IOException
     */
    public boolean login(String host, int port, String username, String password) throws IOException {
        this.ftp.connect(host, port);
        if (FTPReply.isPositiveCompletion(this.ftp.getReplyCode())) {
            if (this.ftp.login(username, password)) {
                this.ftp.setControlEncoding("GBK");
                return true;
            }
        }
        if (this.ftp.isConnected()) {
            this.ftp.disconnect();
        }
        return false;
    }

    /**
     * 关闭数据链接
     *
     * @throws IOException
     */
    public void disConnection() throws IOException {
        if (this.ftp.isConnected()) {
            this.ftp.disconnect();
        }
    }

    /**
     * 递归遍历出目录下面所有文件
     *
     * @param pathName 需要遍历的目录，必须以"/"开始和结束
     * @throws IOException
     */
    public static List<String> List(FTPClient ftp, String pathName) throws IOException {
        List<String> fileList = new ArrayList();
        if (pathName.startsWith("/") && pathName.endsWith("/")) {
            //更换目录到当前目录
            ftp.changeWorkingDirectory(pathName);
            FTPFile[] files = ftp.listFiles();
            for (FTPFile file : files) {
                if (file.isFile()) {
                    fileList.add(pathName + file.getName());

                }
            }
        }
        return fileList;
    }


    /**
     * 递归遍历目录下面指定的文件名
     *
     * @param pathName 需要遍历的目录，必须以"/"开始和结束
     * @param ext      文件的扩展名
     * @throws IOException
     */
    public void List(String pathName, String ext) throws IOException {
        if (pathName.startsWith("/") && pathName.endsWith("/")) {
            this.ftp.changeWorkingDirectory(pathName);
            FTPFile[] files = this.ftp.listFiles();

            for (FTPFile file : files) {
                if (file.isFile()) {
                    if (file.getName().endsWith(ext) && file.getName().contains("MRO")) {
                        arFiles.add(pathName + file.getName());
                    }
                } else if (file.isDirectory()) {
                    if (!".".equals(file.getName()) && !"..".equals(file.getName())) {
                        List(pathName + file.getName() + "/", ext);
                    }
                }
            }

        }
    }


    /**
     * 创建目录以及文件
     */
    public static Boolean creatFile(String filePath, String fileName) {
        File folder = new File(filePath);
        Boolean ff = true;
        //文件夹路径不存在
        if (!folder.exists()) {
            boolean mkdirs = folder.mkdirs();
            System.out.println("Create results" + mkdirs);
        }
        // 如果文件不存在就创建
        File file = new File(fileName);
        if (!file.exists()) {
            System.out.println("File does not exist. Create file:" + fileName);
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            ff = false;
            System.out.println("The file already exists. The file is:" + filePath + fileName);
        }
        return ff;
    }

    /**
     * 从FTP下载文件到本地
     *
     * @param ftpClient     已经登陆成功的FTPClient
     * @param ftpFilePath   FTP上的目标文件路径
     * @param localFilePath 下载到本地的文件路径
     */
    public static void downloadFileFromFTP(FTPClient ftpClient, String ftpFilePath, String localFilePath) {

        InputStream is = null;
        FileOutputStream fos = null;
        try {
            // 获取ftp上的文件
            is = ftpClient.retrieveFileStream(ftpFilePath);
            fos = new FileOutputStream(new File(localFilePath));
            // 文件读取
            int i;
            byte[] bytes = new byte[1024];
            while ((i = is.read(bytes)) != -1) {
                fos.write(bytes, 0, i);

            }

            ftpClient.completePendingCommand();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (fos != null) {
                    fos.close();
                }
                if (is != null) {
                    is.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * 登陆FTP并获取FTPClient对象
     *
     * @return
     */
    public static FTPClient loginFTP(String host, int port, String userName, String password) {
        FTPClient ftpClient = null;
        try {
            ftpClient = new FTPClient();
            // 连接FTP服务器
            ftpClient.connect(host, port);
            // 登陆FTP服务器
            ftpClient.login(userName, password);
            // 中文支持
            ftpClient.setControlEncoding("GBK");
            // 设置文件类型为二进制（如果从FTP下载或上传的文件是压缩文件的时候，不进行该设置可能会导致获取的压缩文件解压失败）
            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
            ftpClient.enterLocalPassiveMode();

            if (!FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {

                System.out.println("Connection FTP failed, user name or password error。");
                ftpClient.disconnect();
            }
//            else {
//                System.out.println("FTP connection successful!");
//            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        return ftpClient;
    }

    //解压方法
    public static List<String> unGZ(String gzFile, String destDir) throws IOException {
        File file = new File(gzFile);
        return unGZ(file, destDir);
    }

    public static List<String> unGZ(File srcFile, String destDir) throws IOException {
        if (StringUtils.isBlank(destDir)) {
            destDir = srcFile.getParent();
        }

        destDir = destDir.endsWith(File.separator) ? destDir : destDir + File.separator;
        List<String> fileNames = new ArrayList<String>();
        InputStream is = null;
        OutputStream os = null;
        try {
            File destFile = new File(destDir, FilenameUtils.getBaseName(srcFile.toString()));
            fileNames.add(FilenameUtils.getBaseName(srcFile.toString()));
            is = new GzipCompressorInputStream(new BufferedInputStream(new FileInputStream(srcFile), BUFFER_SIZE));
            os = new BufferedOutputStream(new FileOutputStream(destFile), BUFFER_SIZE);


            IOUtils.copy(is, os);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            os.close();
            is.close();
        }
        return fileNames;
    }

    /**
     * 解压方法
     *
     * @param compressFile 解压文件
     * @return
     * @throws Exception
     */
    public static File unCompress(String compressFile) throws Exception {

        String upperName = compressFile.toUpperCase();

        unGZ(compressFile, destDir);
        String newName = compressFile.replace("\\", ";");
//        new File(compressFile).delete();
        //newName.substring(newName.lastIndexOf(";")+1,newName.length()-3)
        String xmlstr = destDir + newName.substring(newName.lastIndexOf(";") + 1, newName.length() - 3);
        //xml绝对路径
        List<String> list = null;
        try {
            //  list = xml.parserNode(new File(xmlstr));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new File(xmlstr);

    }

    public static void main(String[] args) {
        try {
            String fileName = "/0409_t/demo112.txt";
            InputStream isFile = null;
            FTPClient ftpClient = FtpUtil.loginFTP("10.78.137.47", 21, "fdaijfbd", "fdaijfbd!@#");
            String dir = fileName.substring(0, fileName.lastIndexOf("/") + 1);
            String file = fileName.substring(fileName.lastIndexOf("/") + 1);
            try {
                ftpClient.changeWorkingDirectory(dir);
                isFile = ftpClient.retrieveFileStream(file);//得到下载信息
                if (isFile == null || ftpClient.getReplyCode() == FTPReply.FILE_UNAVAILABLE) {
                    System.out.println("skip " + fileName);
                }
                byte[] bytes = org.apache.commons.io.IOUtils.toByteArray(isFile);//将流转化为byte数组
                System.out.println(Arrays.toString(bytes));
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (isFile != null) {
                    isFile.close();
                }
                ftpClient.completePendingCommand();
                try {
                    ftpClient.disconnect();
                } catch (Exception ignored) {
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
