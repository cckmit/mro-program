package mro.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @program: cmzj-data-mro-parent
 * @description: 时间处理
 * @author: sd
 * @create: 2021-04-10 18:21
 **/
public class DateUtil {

    /**
     * @return [java.lang.String, java.lang.String]
     * @Author 刘晓雨
     * @Description //将日期转换为时间
     * @Date 23:41 2021/4/20
     * @Param [timeStr, format]
     **/
    public static long toTimestamp(String timeStr, String format) {
        long time = 0;
        ThreadLocal<SimpleDateFormat> threadLocal = ThreadLocal.withInitial(() -> new SimpleDateFormat(format));
        try {
            Date date = threadLocal.get().parse(timeStr);
            time = date.getTime();
        } catch (Exception ex) {
            System.out.println("当前时间转换时间戳有异常,异常时间为：" + timeStr + "," + "异常转换格式：" + format + "，报错信息为：" + ex.getMessage());
        }
        return time;
    }
}
