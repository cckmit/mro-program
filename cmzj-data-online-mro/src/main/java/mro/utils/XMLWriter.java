package mro.utils;

import java.io.*;
import java.util.zip.GZIPOutputStream;

/**
 * @author : zhangbinbin
 * @date :  2020/4/14  14:10
 * @description: 开发测试
 */
public class XMLWriter {

    public static void writeStringFile(String Data, String filePath) {
        BufferedReader bufferedReader = null;
        BufferedWriter bufferedWriter = null;
        File distFile = null;
        try {
            distFile = new File(filePath);
            if (!distFile.getParentFile().exists()){
                distFile.getParentFile().mkdirs();
            }
            bufferedReader = new BufferedReader(new StringReader(Data));
            bufferedWriter = new BufferedWriter(new FileWriter(distFile));
            char buf[] = new char[1024]; // 字符缓冲区
            int len;
            while ((len = bufferedReader.read(buf)) != -1) {
                bufferedWriter.write(buf, 0, len);
            }
            bufferedWriter.flush();
            bufferedReader.close();
            bufferedWriter.close();
        } catch (Exception e) {
            writeStringFile(Data,filePath);
        }
    }
    public static void getFile(InputStream is,String fileName) throws IOException{
        BufferedInputStream in=null;
        BufferedOutputStream out=null;
        in=new BufferedInputStream(is);
        out=new BufferedOutputStream(new FileOutputStream(fileName));
        int len=-1;
        byte[] b=new byte[1024];
        while((len=in.read(b))!=-1){
            out.write(b,0,len);
        }
        in.close();
        out.close();
    }


    public static byte[] compress(String str, String encoding) {
        if (str == null || str.length() == 0) {
            return null;
        }
        byte[] buff={};
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip;
        try {
            gzip = new GZIPOutputStream(out);
            gzip.write(str.getBytes(encoding));
            gzip.close();
            buff=out.toByteArray();

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if(out!=null){
                    out.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }

        return buff;
    }

    public static void writeBytesToFile(String filePath,byte[] bytes) throws IOException{
        OutputStream out = new FileOutputStream(filePath);
        InputStream is = new ByteArrayInputStream(bytes);
        byte[] buff = new byte[1024];
        int len = 0;
        while((len=is.read(buff))!=-1){
            out.write(buff, 0, len);
        }
        is.close();
        out.close();
    }

}
