package mro.cgi;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author hao
 * @date 2021-01-25 16:30
 */
public class Sink2Mysql extends RichSinkFunction<String> {
    public static Long tranTimeToLong(String tm) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = format.parse(tm);
        return date.getTime();
    }

    String[] split;
    String datatime;
    Long datatimestamp;
    Long nowtime;
    String sql;
    Connection connection;
    PreparedStatement preparedStatement = null;
    private static SimpleDateFormat sf = null;


    @Override
    public void open(Configuration parameters) throws Exception {
        sql = "insert into mro_sample_delay(DATA_TIME,TIME,TIME_DELAY) values(?,?,?)";
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        nowtime = System.currentTimeMillis();
        split = value.trim().split(" ", -1);
        if (split[3].length() == 23) {
            datatime = split[3].replace("T", " ");
            datatimestamp = tranTimeToLong(datatime);
            try {
                if (connection == null || connection.isClosed()) {
                    connection = getMysqlConnection();
                }
                preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setString(1, datatime);
                preparedStatement.setString(2, getTimeStampToString(nowtime));
                preparedStatement.setString(3, String.valueOf((nowtime - datatimestamp) / 1000));
                preparedStatement.executeUpdate();
                preparedStatement.close();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }

        }

    }

    @Override
    public void close() throws Exception {
        preparedStatement.close();
        connection.close();
    }

    public static String getTimeStampToString(long time) {
        Date d = new Date(time);
        sf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sf.format(d);
    }

    public static Connection getMysqlConnection() {
        final String mysqlDriver = "com.mysql.jdbc.Driver";
        final String mysqlUser = "mr_info";
        final String mysqlPassword = "mr_info1q#";
        final String mysqlUrl = "jdbc:mysql://10.76.217.186:3306/mr_info?serverTimezone=UTC";
        Connection connection = null;
        try {
            Class.forName(mysqlDriver);
            connection = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }


}
