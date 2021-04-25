package mro.stream.parser.eric;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * @ClassName EricProcessStream
 * @Description //TODO
 * @Author 刘晓雨
 * @Date 2021/4/20 22:36
 * @Version 1.0
 **/
public class EricProcessStream extends BroadcastProcessFunction<Map<String, String>, Map<String, String>, String> {
    /**
     * 数据匹配,匹配之后解析入kafka
     */
    SAXReader saxReader;
    StringReader sr;
    Document document;
    List<Element> urls;
    Map<String, List<String>> mapA;
    Map<String, List<String>> mapB;
    String mapKey = "";
    List<String> mapValue = new ArrayList<>();
    int id = 0;
    String city_id;
    int sum = 0;
    int partitionTo = 0;
    List<String> listFile = new ArrayList<>(4000);
    /**
     * 广播的数据存储在Map中
     */
    private Map<String, String> keywords = new HashMap<>();

    @Override
    public void processElement(Map<String, String> value, ReadOnlyContext readOnlyContext, Collector<String> collector) {
        if (!"filePath".equals(value.get("filePath"))) {  //？fliepath=文件绝对路径？
            String enodeb_id = value.get("filePath").split("_")[4];//从流中获取基站编号
            /**数据整理*/
            city_id = keywords.get(enodeb_id); ////基站编号跟广播流的进行匹配，得到城市编号
            if (city_id == null) {
                city_id = "";
            }

            try {
                saxReader = new SAXReader();
                sr = new StringReader(value.get("dataContent"));
                document = saxReader.read(sr);
                urls = document.selectNodes("bulkPmMrDataFile/eNB/measurement");
                if (urls.size() == 1) {
                    List<String> colors = Stream.of("NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL").collect(toList());
                    String replace = colors.toString().replaceAll(",", " ").replace("[", "").replace("]", "");
                    mapA = nodeAnaMeasurement(urls.get(0), "A");
                    for (Map.Entry<String, List<String>> entry : mapA.entrySet()) {
                        mapKey = entry.getKey();
                        mapValue = entry.getValue();
                        for (String str : mapValue) {
                            id = Integer.parseInt(str.split(" ")[4]);
                            listFile.add((str + " " + replace + " NIL").trim().replace("NIL", "") + " " + enodeb_id + " " + city_id + " 460-00" + "-" + (id / 256) + "-" + (id % 256));
                            sum++;
                            if (sum > 3000) {               //累计3000一批发送出去
                                if (partitionTo == 512) {
                                    partitionTo = 0;
                                }   //跟下面的分区有关
//                                                System.out.println(listFile.toString().replace("[", "").replace("]", "").trim() + "=" + partitionTo); //+"="+partitionTo
                                collector.collect(listFile.toString().replace("[", "").replace("]", "").trim() + "=" + partitionTo); //+"="+partitionTo
                                partitionTo++;
                                listFile.clear();
                                sum = 1;
                            }
                        }
                    }
                } else if (urls.size() > 1) {
                    mapA = nodeAnaMeasurement(urls.get(0), "A");
                    mapB = nodeAnaMeasurement(urls.get(1), "B");
                    for (Map.Entry<String, List<String>> entry : mapA.entrySet()) {
                        mapKey = entry.getKey();
                        mapValue = entry.getValue();
                        for (String str : mapValue) {
                            //xml一共有三段，只有三段的都有才能继续
                            if (mapB.get(mapKey) != null) {
                                id = Integer.parseInt(str.split(" ")[4]);
                                listFile.add((str + " " + mapA.get(mapKey).get(0) + " NIL").trim().replace("NIL", "") + " " + enodeb_id + " " + city_id + " 460-00" + "-" + (id / 256) + "-" + (id % 256));
                                sum++;
                                if (sum > 3000) {               //累计3000一批发送出去
                                    if (partitionTo == 512) {
                                        partitionTo = 0;
                                    }   //跟下面的分区有关
//                                                System.out.println(listFile.toString().replace("[", "").replace("]", "").trim() + "=" + partitionTo); //+"="+partitionTo
                                    collector.collect(listFile.toString().replace("[", "").replace("]", "").trim() + "=" + partitionTo); //+"="+partitionTo
                                    partitionTo++;
                                    listFile.clear();
                                    sum = 1;
                                }
                            }
                        }
                    }
                }
                if (partitionTo == 512) {
                    partitionTo = 0;
                }
                collector.collect(listFile.toString().replace("[", "").replace("]", "").trim() + "=" + partitionTo);//+"="+partitionTo
                partitionTo++;
            } catch (Exception e) {
                System.out.println("Parse error：" + value.get("filePath") + "," + e.toString());
            } finally {
                listFile.clear();
            }
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
        List<Element> eleLists = element.elements();
        try {
            eleLists.stream().filter(e -> !"smr".equals(e.getName())).forEach(el -> {
                List<Attribute> attrList = el.attributes();
                String key = attrList.stream().map(attribute -> attribute.getName() + ":" + attribute.getValue() + "|").collect(Collectors.joining());
                /** collectVal collectValue 重复  */
                List<Attribute> sortList1 = attrList.stream().sorted(Comparator.comparing(Node::getName)).collect(Collectors.toList());
                String collectVal = sortList1.stream().map(attribute -> attribute.getValue() + " ").collect(Collectors.joining());
                List<Element> eLists = el.elements();
                List<String> vList = new ArrayList<>();
                eLists.forEach(ele -> {
                    if (model.equals("A")) {
                        String[] elementValues = ele.getText().trim().split(" ");
                        String[] sortedResultStr = new String[29];
                        int[] indexs = {0, 1, 2, 3, 8, 9, 10, 11, 4, 5, 6, 7, 20, 21, 22, 23, 24, 25, 26, 12, 13, 14, 15, 19, 16, 17, 27, 28};
                        for (int i = 0; i < elementValues.length; i++) {
                            sortedResultStr[indexs[i]] = elementValues[i];
                        }
                        sortedResultStr[18] = "NIL";
                        vList.add(collectVal + StringUtils.join(sortedResultStr, " ").trim());
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
}
