package mro.stream.parser.zte;

import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @ClassName ZteProcessStream
 * @Description //TODO
 * @Author 刘晓雨
 * @Date 2021/4/20 23:16
 * @Version 1.0
 **/
public class ZteProcessStream extends BroadcastProcessFunction<Map<String, String>, Map<String, String>, String> {
    /**
     * 广播的数据存储在Map中
     */
    private Map<String, String> keywords = new HashMap<>();
    /**
     * 数据匹配,匹配之后解析入kafka
     */
    String enodeb_id;
    String city_id;
    SAXReader saxReader;
    StringReader sr;
    Document document;
    List<Element> urls;
    Map<String, List<String>> mapA;
    Map<String, List<String>> mapB;
    int id = 0;
    int i = 0;
    List<String> listFile = new ArrayList<>();
    int sum = 0;
    StringBuilder stringBuilder;

    @Override
    public void processElement(Map<String, String> stringStringMap, ReadOnlyContext readOnlyContext, Collector<String> collector) {
        try {
            /**防止null指针异常*/
            enodeb_id = stringStringMap.get("fileName").split("_")[4];
            city_id = keywords.get(enodeb_id);
            if (city_id == null) {
                city_id = "";
            }
            saxReader = new SAXReader();
            sr = new StringReader(stringStringMap.get("fileString"));
            document = saxReader.read(sr);//读取XML文件,获得document对象
            urls = document.selectNodes("bulkPmMrDataFile/eNB/measurement");//从当前节点的儿子节点中选择名称为 bulkPmMrDataFile/eNB/measurement 的节点
            if (urls.size() == 1) {
                mapA = nodeAnaMeasurement(urls.get(0), "A");
                for (Map.Entry<String, List<String>> entry : mapA.entrySet()) {
                    String mapKey = entry.getKey();
                    List<String> mapValue = entry.getValue();
                    for (String str : mapValue) {
                        String[] strs = str.trim().split(" ");
                        stringBuilder = new StringBuilder();
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
                        id = Integer.parseInt(strs[0]);
                        listFile.add((stringBuilder + " " + mapA.get(mapKey).get(0) + " NIL").trim().replace("NIL", "") + " " + enodeb_id + " " + city_id + " 460-00" + "-" + (id / 256) + "-" + (id % 256));
                        sum++;
                        if (sum > 3000) {
                            if (i == 512) {
                                i = 0;
                            }
                            collector.collect(listFile.toString().replace("[", "").replace("]", "").trim() + "=" + i);
                            i++;
                            sum = 0;
                            listFile.clear();
                        }
                    }
                }
            } else if (urls.size() > 1) {
                mapA = nodeAnaMeasurement(urls.get(0), "A");
                mapB = nodeAnaMeasurement(urls.get(1), "B");
                for (Map.Entry<String, List<String>> entry : mapA.entrySet()) {
                    String mapKey = entry.getKey();
                    List<String> mapValue = entry.getValue();
                    for (String str : mapValue) {
                        if (mapB.get(mapKey) != null) {
                            String[] strs = str.trim().split(" ");
                            stringBuilder = new StringBuilder();
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
                            id = Integer.parseInt(strs[0]);
                            listFile.add((stringBuilder + " " + mapA.get(mapKey).get(0) + " NIL").trim().replace("NIL", "") + " " + enodeb_id + " " + city_id + " 460-00" + "-" + (id / 256) + "-" + (id % 256));
                            sum++;
                            if (sum > 3000) {
                                if (i == 512) {
                                    i = 0;
                                }
                                collector.collect(listFile.toString().replace("[", "").replace("]", "").trim() + "=" + i);
                                i++;
                                sum = 0;
                                listFile.clear();
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("keywords is unll");
        } finally {
            listFile.clear();
        }
    }

    @Override
    public void processBroadcastElement(Map<String, String> value, Context ctx, Collector<String> out) throws Exception {
        /**更新广播数据*/
        keywords = value;
    }

    /**
     * Measurement节点解析
     *
     * @param element
     */
    private static Map<String, List<String>> nodeAnaMeasurement(Element element, String model) {
        Map<String, List<String>> map = new HashMap<>();
        List<Element> eleLists = element.elements();//获得measurement下的所有子节点
        try {
            //找出子节点为smr的节点进行遍历
            eleLists.stream().filter(e -> !"smr".equals(e.getName())).forEach(el -> {
                List<Attribute> attrList = el.attributes(); //获取smr的所有属性
                //smr节点下面的属性的名字，属性值   属性名:属性值| 属性名:属性值| 属性名:属性值
                String key = attrList.stream().map(attribute -> attribute.getName() + ":" + attribute.getValue() + "|").collect(Collectors.joining());
                /** collectVal collectValue 重复  */
                // 属性值  属性值
                String collectVal = attrList.stream().map(attribute -> attribute.getValue() + " ").collect(Collectors.joining());
                //遍历el下面的所有子节点
                List<Element> eLists = el.elements();

                List<String> vList = new ArrayList<>();
                eLists.forEach(ele -> {
                    //"rewrite-"
                    if (model.equals("A")) {
                        vList.add(collectVal + ele.getText().trim());  //上一个节点的属性值+属性文本内容
                    } else {
                        vList.add(ele.getText().trim().trim());
                    }
                });
                map.put(key, vList); //属性名:属性值,  属性值 下一个节点的文本内容
            });
        } catch (Exception e) {
            System.out.println("Parse error");
        }
        return map;
    }
}
