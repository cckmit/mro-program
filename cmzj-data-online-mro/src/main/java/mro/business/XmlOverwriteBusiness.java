package mro.business;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;
import mro.service.FileMoveService;
import mro.utils.JDBCUtil;
import mro.utils.XMLWriter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

/**
 * @author : Mr li
 * @date :  2020/6/1  14:22
 * @description: XML 改写回传
 */
public class XmlOverwriteBusiness implements Serializable{
    private static PreparedStatement stmt = null;
    private  static String Rewrite_data=null;
    /**
     * 回传集团
     * @param
     * @throws Exception
     */
    /**垃圾回收*/
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
    }
    /**
     * 改写主要逻辑，接文件字节过来进行文件的改下判断
     * @param map
     * @return
     * @throws Exception
     */
    public String xmlOverwrite(Map<String, String> map) throws Exception {
        FileMoveService sftp = new FileMoveService();
        /**连接sftp*/
        // 10.212.116.92;22;Szsx%whwx9102;/export/home/omc/var/fileint/TSNBI/LTESpecial/;ossuser
        //"10.212.102.141;22;ossuser;Bin20%18go;/export/home/omc/var/fileint/TSNBI/LTESpecial/"
        String Ts=map.get("Ts");
        String[] Tss=Ts.split(";");
        //Session session= sftp.login("ljc", "123456", "192.168.128.1", 22);
        Session session= sftp.login(Tss[2],Tss[3], Tss[0], Integer.parseInt(Tss[1]));
        Channel channel = session.openChannel("sftp");
        channel.connect();
        //sftp对象
        ChannelSftp sftpobject = (ChannelSftp) channel;
        String text = map.get("dataContent");
        String filePath = map.get("filePath");
        System.out.println("文件="+filePath);
        String name="";
        if (filePath != null) {
            String[] str_name=filePath.split("/");
            name=str_name[str_name.length-3]+"/"+str_name[str_name.length-2]+"/"+str_name[str_name.length-1];
            /**清空内存*/
            str_name=null;
        } else {
            /** TODO 缺少对异常分支的处理 */
        }
        Long startTime = System.currentTimeMillis();
        checkRegulate(text, filePath);
        String resultStr =Rewrite_data;
        /**空间释放*/
        Rewrite_data=null;
        /**
         * TODO temp_test
         */
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        GZIPOutputStream outCompress = new GZIPOutputStream(os);
        outCompress.write(resultStr.getBytes("utf-8"));
        /************/
        byte[] bytess= XMLWriter.compress(resultStr,"utf-8");

        System.out.println("bytess = " + bytess.length);
        /**上传路径拼接*/
        String fileNameTemp = Tss[4] +name;
        try {
            /**TODO 路径为替换 */
            /**上传时间*/
            Long ftpTime=System.currentTimeMillis();
            sftp.upload(fileNameTemp,bytess,sftpobject);
            System.out.println("FTP文件上传耗时："+(System.currentTimeMillis()-ftpTime));
            bytess=null;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            outCompress.finish();
            outCompress.flush();
            outCompress.close();
            outCompress=null;
            if (os != null)
                os.close();
            os=null;
            if (sftpobject != null)
                sftpobject.disconnect();
            if (channel != null)
                channel.disconnect();
            if (session != null)
                session.disconnect();
        }
        System.out.println("改写回传时间：" + String.valueOf(System.currentTimeMillis() - startTime));
        /**对象回收*/
        sftp=null;
        System.gc();
        return resultStr;

    }

    /**
     * 查询该文件满足的规则
     * @param fileName 文件名称
     * @throws Exception
     */
    private void checkRegulate(String text, String fileName) throws Exception {
        String bh = fileName.split("_")[4];    //获取文件ip进入数据库查询具体满足条件进行那个规则修改
        boolean qd = false;

        /** TODO 该SQL是否可以优化 */
        Long startTime=System.currentTimeMillis();

        stmt = JDBCUtil.getConnection().prepareStatement("SELECT * FROM mr_enodeb where Serial_number =" + "'" + bh + "'");

        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
            qd = true;
        }
        Long resTime=System.currentTimeMillis();
        if(qd){
            regulateFirst(text);
        }else {
            regulateSecond(text);
        }
        System.out.println("resTime："+(System.currentTimeMillis()-resTime));
        /**释放对象提示回收*/
        bh=null;
    }

    /**
     * 匹配是否满足规则1
     *
     * @param str
     * @throws Exception
     */
    private void regulateFirst(String str) throws Exception {
        InputStream inputStream = new ByteArrayInputStream(str.getBytes());
        OutputStream outputStream = new ByteArrayOutputStream();
        //保存cgi改写的判断数据
        Map<String,String>cacheMap=new HashMap<>();
        //获取cgi修改判断数据
        stmt = JDBCUtil.getConnection().prepareStatement("SELECT * FROM MR_cgi");
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
            // cacheMap.put(rs.getString("id"),rs.getString("LteNcEarfcn"));
            cacheMap.put(rs.getString("id")+","+rs.getString("LteNcEarfcn"),"1");
        }

        stmt.close();
        //随机数对象
        Random random = new Random();
        //①获得解析器DocumentBuilder的工厂实例DocumentBuilderFactory  然后拿到DocumentBuilder对象
        DocumentBuilder newDocumentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        //②获取一个与磁盘文件关联的非空Document对象
        Document doc = newDocumentBuilder.parse(inputStream);
        //③通过文档对象获得该文档对象的根节点
        Element root = doc.getDocumentElement();
        //查找指定节点名称
        NodeList personList2 = root.getElementsByTagName("measurement");
        //这里获取第1个节点
        Node item2 = personList2.item(0);
        //获取personElement2下面的子节点
        Element personElement2 = (Element) item2;
        //通过personElement2下面的子节点获得孙子节点
        NodeList personList = personElement2.getElementsByTagName("object");
        //获取孙子节点的长度
        int str_leng=personList.getLength();
        //创建保存要修改的LteNcEarfcnMap的判断数据集合
        Map<String,Integer> LteNcEarfcnMap=new HashMap<>(str_leng);
        String [] vValArr=null;
        Element personElement =null;
        NodeList nameList =null;
        Node item=null;
        for (int i = 0; i < str_leng; i++) {
            item = personList.item(i);
            personElement = (Element) item;
            nameList = personElement.getElementsByTagName("v");
            // 业务逻辑修改XML文件
            for (int j = 0; j < nameList.getLength(); j++) {
                vValArr=nameList.item(j).getTextContent().split(" ");
                if (cacheMap.get(personElement.getAttribute("id")+","+vValArr[6])!=null) {
                    if(!vValArr[0].equals("NIL")&&Integer.parseInt(vValArr[0])<30){
                        vValArr[0]=(random.nextInt(50) % (21) + 30) + "";
                        vValArr[1]=(random.nextInt(27) % (26) + 1) + "";
                    }else {
                        vValArr[1]=(random.nextInt(27) % (26) + 1) + "";
                    }
                } else {
                    if ((!vValArr[0].equals("NIL")) && Integer.parseInt(vValArr[0]) < 30) {
                        vValArr[0]=(random.nextInt(50) % (21) + 30)+"";
                    }

                }
                nameList.item(j).setTextContent(String.join(" ",vValArr));
                /**"V"的拆分数组提示GC回收*/
                vValArr=null;
            }
            /**提示gc回收对象释放内存*/
            item=null;
            personElement=null;
            nameList=null;

        }

        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        Source source = new DOMSource(doc);
        Result result = new StreamResult(outputStream);
        transformer.transform(source, result);//将 XML==>Source 转换为 Result
        /**手动提示释放空间*/
        newDocumentBuilder=null;
        doc=null;
        root=null;
        personList2=null;
        item2=null;
        personElement2=null;
        personList=null;
        inputStream.close();
        inputStream=null;
        outputStream.close();
        System.gc();
        Rewrite_data=outputStream.toString();
        outputStream=null;
    }


    /**
     * 匹配是否满足规则 2
     *
     * @param
     * @throws Exception
     */
    private void regulateSecond(String text) throws Exception {

        OutputStream outputStream = new ByteArrayOutputStream();
        InputStream inputStream = new ByteArrayInputStream(text.getBytes());
        Map<String,String>cacheMap=new HashMap<>();

//        stmt = JDBCUtils.getConnection().prepareStatement("SELECT * FROM MR_cgi");
        stmt = JDBCUtil.getConnection().prepareStatement("SELECT * FROM MR_cgi");
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
            //  cacheMap.put(rs.getString("id"),rs.getString("LteNcEarfcn"));
            cacheMap.put(rs.getString("id")+","+rs.getString("LteNcEarfcn"),"1");
        }
        stmt.close();
        //随机数对象
        Random random = new Random();
        //获得解析器DocumentBuilder的工厂实例DocumentBuilderFactory  然后拿到DocumentBuilder对象
        DocumentBuilder newDocumentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        //获取一个与磁盘文件关联的非空Document对象
        Document doc = newDocumentBuilder.parse(inputStream);
        //通过文档对象获得该文档对象的根节点
        Element root = doc.getDocumentElement();
        //查找指定节点名称
        NodeList personList2 = root.getElementsByTagName("measurement");
        //这里获取第1个节点
        Node item2 = personList2.item(0);
        //获取personElement2下面的子节点
        Element personElement2 = (Element) item2;
        //通过根节点获得子节点
        NodeList personList = personElement2.getElementsByTagName("object");
        //这里获取第1个节点
        int str_leng=personList.getLength();
        Map<String,Integer> LteNcEarfcnMap=new HashMap<>(str_leng);
        String [] vValArr=null;
        Element personElement =null;
        NodeList nameList =null;
        for (int i = 0; i < str_leng; i++) {
            personElement = (Element) personList.item(i);
            nameList = personElement.getElementsByTagName("v"); //获取object下面的子节点
            for (int j = 0; j < nameList.getLength(); j++) {
                vValArr=nameList.item(j).getTextContent().split(" ");
                if (cacheMap.get(personElement.getAttribute("id")+","+vValArr[6])!=null) {
                    vValArr[1]=(random.nextInt(27) % (26) + 1) + "";
                    nameList.item(j).setTextContent(String.join(" ",vValArr));
                }
            }
        }


        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        Source source = new DOMSource(doc);
        Result result = new StreamResult(outputStream);
        transformer.transform(source, result);//将 XML==>Source 转换为 Result
        /**手动提示释放空间*/
        newDocumentBuilder=null;
        doc=null;
        root=null;
        personList2=null;
        item2=null;
        personElement2=null;
        personList=null;
        inputStream.close();
        inputStream=null;
        outputStream.close();
        System.gc();
        Rewrite_data=outputStream.toString();
        outputStream=null;

    }

}
