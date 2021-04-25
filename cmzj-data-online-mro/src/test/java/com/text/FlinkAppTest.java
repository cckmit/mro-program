package com.text;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * @ClassName FlinkAppTest
 * @Description //TODO
 * @Author 刘晓雨
 * @Date 2021/4/21 0:21
 * @Version 1.0
 **/
public class FlinkAppTest {

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
                String key = attrList.stream().map(attribute -> attribute.getName() + ":" + attribute.getValue() + "|")
                        .collect(Collectors.joining());
                /** collectVal collectValue 重复  */
                String collectVal = attrList.stream().map(attribute -> attribute.getValue() + " ").collect(Collectors.joining());
                List<Element> eLists = el.elements();
                List<String> vList = new ArrayList<>();
                eLists.forEach(ele -> {
                    if (model.equals("A")) {
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

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.addSource(new RichSourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int sum = 0;
                int partitionTo = 0;
                List<String> listFile = new ArrayList<>(4000);

                File file = new File("D:\\InternetAPP\\IDEA-2020\\IdeaProject\\mro-program\\cmzj-data-online-mro\\src\\main\\resources\\FDD-LTE_MRO_HUAWEI_172031030135_461351_20210424091500.xml");
                InputStream in = new FileInputStream(file);
                byte b[] = new byte[(int) file.length()];
                int len = 0;
                int temp = 0; //全部读取的内容都使用temp接收
                while ((temp = in.read()) != -1) { //当没有读取完时，继续读取
                    b[len] = (byte) temp;
                    len++;
                }
                in.close();

                SAXReader saxReader = new SAXReader();
                StringReader sr = new StringReader(new String(b, 0, len));
                Document document = saxReader.read(sr);
                List<Element> urls = document.selectNodes("bulkPmMrDataFile/eNB/measurement");
                //MmeCode:190|MmeGroupId:421|MmeUeS1apId:268677033|TimeStamp:2021-04-16T09:27:17.760|id:263719555|--->[190 421 268677033 2021-04-16T09:27:17.760 263719555 54 36 24 0 38400 303 38400 186 3 47 NIL 30 13 9 0 0 0 0 0 32 NIL NIL NIL NIL NIL NIL NIL NIL NIL]
                Map<String, List<String>> mapA = nodeAnaMeasurement(urls.get(0), "A");
                Map<String, List<String>> mapB = nodeAnaMeasurement(urls.get(1), "B");
                for (Map.Entry<String, List<String>> entry : mapA.entrySet()) {
                    String mapKey = entry.getKey();
                    List<String> mapValue = entry.getValue();
                    if (mapB.get(mapKey) != null) {
                        for (String str : mapValue) {
                            int id = Integer.parseInt(str.split(" ")[4]);
                            System.out.println((str + " " + mapB.get(mapKey).get(0) + " NIL").trim().replace("NIL", "") + " enodeb_id" + " city_id" + " 460-00" + "-" + (id / 256) + "-" + (id % 256));
                            listFile.add((str + " " + mapB.get(mapKey).get(0) + " NIL").trim().replace("NIL", "") + " enodeb_id" + " city_id" + " 460-00" + "-" + (id / 256) + "-" + (id % 256));
                            sum++;
                            if (sum > 3000) {               //累计3000一批发送出去
                                if (partitionTo == 512) {
                                    partitionTo = 0;
                                }   //跟下面的分区有关
                                ctx.collect(listFile.toString().replace("[", "").replace("]", "").trim() + "=" + partitionTo); //+"="+partitionTo
                                partitionTo++;
                                listFile.clear();
                                sum = 1;
                            }
                        }
                    }
                }

                if (partitionTo == 512) {
                    partitionTo = 0;
                }
//                ctx.collect(listFile.toString().replace("[", "").replace("]", "").trim() + "=" + partitionTo);//+"="+partitionTo
            }

            @Override
            public void cancel() {

            }
        });

        source.map(line -> {
            if (line.equals("A")) {
                System.out.println("存在：" + line);
            } else {
                System.out.println("存在：" + line);
            }
            return line;
        }).print();


        env.execute("Test");
    }
}
