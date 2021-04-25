package mro.business;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author : lijichen
 * @date :  2020/3/30  16:36
 * @description: 原生XML解析
 * TODO 性能优化结束，需删除耗时输出
 */
public class XmlParserBusiness {

    static XmlParserBusiness xml = new XmlParserBusiness();
    static final String MODEL_TYPE = "A";
    /**
     * Measurement节点解析
     * @param element
     */
    private Map<String, List<String>> nodeAnaMeasurement(Element element, String model) {
        Map<String, List<String>> map = new HashMap<>();
        List<Element> eleLists = element.elements();
        try {
            eleLists.stream().filter(e -> !"smr".equals(e.getName())).forEach(el -> {
                List<Attribute> attrList = el.attributes();
                String key = attrList.stream().map(attribute -> attribute.getName() + ":" + attribute.getValue() + "|").collect(Collectors.joining());
                /** collectVal collectValue 重复  */
                String collectVal = attrList.stream().map(attribute -> attribute.getValue() + " ").collect(Collectors.joining());
                List<Element> eLists = el.elements();
                List<String> vList = new ArrayList<>();
                eLists.forEach(ele -> {
                    if (MODEL_TYPE.equals(model)) {
                        vList.add(collectVal + ele.getText().trim());
                    } else {
                        vList.add(ele.getText().trim().trim());
                    }
                });
                map.put(key, vList);
            });
        } catch (Exception e) {
            System.out.println("Parse error---->");
        }
        return map;
    }

    /**
     * 基于String进行处理
     * @return
     * @throws DocumentException
     * @throws IOException
     */
    public static List<String> parserNodeString(String xmlStr,String enodeb_id,String city_id) throws DocumentException, IOException {
        List<String> reList = new ArrayList<>(300000);
       /**数据整理*/
        try { if(city_id==null){city_id="";} }catch (Exception e){ city_id="";}
        try {
            SAXReader saxReader = new SAXReader();
            StringReader sr = new StringReader(xmlStr);
            Document document = saxReader.read(sr);
            List<Element> urls = document.selectNodes("bulkPmMrDataFile/eNB/measurement");
            Map<String, List<String>> mapA = xml.nodeAnaMeasurement(urls.get(0), "A");
            Map<String, List<String>> mapB = xml.nodeAnaMeasurement(urls.get(1), "B");
            String mapKey="";
            List<String> mapValue=new ArrayList<>();
            for (Map.Entry<String, List<String>> entry : mapA.entrySet()) {
                mapKey = entry.getKey();
                mapValue = entry.getValue();
                for (String str : mapValue) {
                    if (mapB.get(mapKey) != null) {
                        reList.add((str + " " + mapB.get(mapKey).get(0) + " NIL").trim().replace("NIL","")+" "+enodeb_id+" "+city_id);//.replaceAll("T"," ")
                    }
                }
            }
        }catch (Exception e){
            try {
                System.out.println("Parse error");
                reList.clear();
                reList.add("LJC");
                return reList;
            }catch (Exception e1){
            }
        }
            return reList;
    }
}
