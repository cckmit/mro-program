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
 * @program: cmzj-data-online-mro
 * @description: 中兴的解析程序
 * @author: Mr.Li
 * @create: 2020-07-21 10:15
 **/
public class ZX_XmlParserBusiness {
    static final String MODEL_TYPE = "A";
    /**
     * Measurement节点解析
     *
     * @param element
     */
    private static Map<String, List<String>> nodeAnaMeasurement(Element element, String model) {
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
                    //"rewrite-"
                    if (MODEL_TYPE.equals(model)) {
                        vList.add(collectVal + ele.getText().trim());
                    } else {
                        vList.add(ele.getText().trim().trim());
                    }
                });
                map.put(key, vList);
            });
        } catch (Exception e) {
            System.out.println("Parse error");
        }
        return map;
    }

    /**
     * 基于String进行处理
     * @return
     * @throws DocumentException
     * @throws IOException
     */
    public static List<String> parserNodeString(Map<String,String> StrMap) throws DocumentException, IOException {
        List<String> reList = new ArrayList<>(300000);
        String enodeb_id=StrMap.get("enodeb_id");
        String city_id=StrMap.get("city_id");
        try {
            if(city_id.equals(null)){city_id="";}
        }catch (Exception e){
            city_id="";
        }

        try {
            SAXReader saxReader = new SAXReader();
            StringReader sr = new StringReader(StrMap.get("fileString"));
            Document document = saxReader.read(sr);
            List<Element> urls = document.selectNodes("bulkPmMrDataFile/eNB/measurement");
            Map<String, List<String>> mapA = nodeAnaMeasurement(urls.get(0), "A");
            Map<String, List<String>> mapB = nodeAnaMeasurement(urls.get(1), "B");
            for (Map.Entry<String, List<String>> entry : mapA.entrySet()) {
                String mapKey = entry.getKey();
                List<String> mapValue = entry.getValue();
                for (String str : mapValue) {
                    if (mapB.get(mapKey) != null) {
                        String []strs=str.trim().split(" ");
                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.append(strs[2]).append(" ").append(strs[3]).append(" ").append(strs[1])
                                .append(" ").append(strs[4]).append(" ").append(strs[0])
                                .append(" ").append(strs[7]).append(" ").append(strs[23])
                                .append(" ").append(strs[8]).append(" ").append(strs[24])
                                .append(" ").append(strs[5]).append(" ").append(strs[6])
                                .append(" ").append(strs[21]).append(" ").append(strs[22])
                                .append(" ").append(strs[9]).append(" ").append(strs[10])
                                .append(" ").append(strs[11]).append(" ").append(strs[12])
                                .append(" ").append(strs[13]).append(" ").append(strs[14])
                                .append(" ").append(strs[15]).append(" ").append(strs[16])
                                .append(" ").append(strs[19]).append(" ").append(strs[20])
                                .append(" ").append(strs[18]).append(" ").append(strs[17])
                                .append(" ").append(strs[30]).append(" ").append(strs[31])
                                .append(" ").append(strs[28]).append(" ").append(strs[29])
                                .append(" ").append(strs[27]).append(" ").append(strs[25])
                                .append(" ").append(strs[26]).append(" ").append("NIL")
                                .append(" ").append("NIL");
                        reList.add((stringBuilder + " " + mapB.get(mapKey).get(0) + " NIL").trim().replace("NIL","")+" "+enodeb_id+" "+city_id);
                    }
                }
            }
        }catch (Exception e){
            System.out.println("Throw away a batch of data");
            return new ArrayList<>();
        }finally {
            return reList;
        }
    }


}
