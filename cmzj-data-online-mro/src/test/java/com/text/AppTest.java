package com.text;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.*;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * @ClassName AppTest
 * @Description //TODO
 * @Author 刘晓雨
 * @Date 2021/4/20 23:45
 * @Version 1.0
 **/
public class AppTest {
    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd 08:00:00");
    private static long time_15 = System.currentTimeMillis();

    /**
     * Measurement节点解析
     */
    private static Map<String, List<String>> nodeAnaMeasurement(Element element, String model) {
        Map<String, List<String>> map = new HashMap<>();
        List<Element> eleLists = element.elements();
        try {
            eleLists.stream().filter(e -> !"smr".equals(e.getName())).forEach(el -> {
                List<Attribute> attrList = el.attributes();
                String key = attrList.stream().map(attribute -> attribute.getName() + ":" + attribute.getValue() + "|").collect(Collectors.joining());
                /** collectVal collectValue 重复  */
                List<Attribute> sortList1 = attrList.stream().sorted(Comparator.comparing(Node::getName)).collect(toList());
                String collectVal = sortList1.stream().map(attribute -> attribute.getValue() + " ").collect(Collectors.joining());
                List<Element> eLists = el.elements();
                List<String> vList = new ArrayList<>();
                eLists.forEach(ele -> {
                    if (model.equals("A")) {
                        String[] elementValues = ele.getText().trim().split(" ");
                        String[] sortedResultStr = new String[29];
                        int[] indexs = {4, 5, 0, 2, 19, 8, 16, 9, 10, 11, 12, 13, 14, 15, 18, 17, 6, 7, 1, 3, 25, 26, 24, 22, 23, 20, 21};
                        for (int i = 0; i < elementValues.length; i++) {
                            sortedResultStr[indexs[i]] = elementValues[i];
                        }
                        sortedResultStr[27] = "NIL";
                        sortedResultStr[28] = "NIL";
                        vList.add(collectVal + StringUtils.join(sortedResultStr, " ").trim());
                    } else {
                        vList.add(ele.getText().trim().trim());
                    }
                });
                map.put(key, vList);
            });
        } catch (Exception e) {
            System.out.println("Parse error---->" + e.toString());
        }
        return map;
    }

    public static void main(String[] args) throws IOException, DocumentException {
        List<String> colors = Stream.of("NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL").collect(toList());
        String replace = colors.toString().replaceAll(",", " ").replace("[", "").replace("]", "");
        System.out.println(replace);
    }
}
