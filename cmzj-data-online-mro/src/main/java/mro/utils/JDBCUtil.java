package mro.utils;

import oracle.jdbc.driver.OracleDriver;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/***
 * Oracle
 */
public class JDBCUtil {


    public String getDataSource() {
        return DataSource;
    }

    public void setDataSource(String dataSource) {
        DataSource = dataSource;
    }
    private String DataSource = null;

    //表示定义数据库的用户名
    private  static  final String USERNAME = "dataex";
    // 定义数据库的密码
    private static final String PASSWORD= "dataex_1Q#";
    // 定义访问数据库的地址
    private static final String URL = "jdbc:oracle:thin:@(DESCRIPTION_LIST=(LOAD_BALANCE = off)(FAILOVER = on)(DESCRIPTION=(ADDRESS_LIST=(LOAD_BALANCE=OFF)(FAILOVER=ON)(ADDRESS = (PROTOCOL = TCP)(HOST =20.26.90.23)(PORT = 1521)))(CONNECT_DATA =(SERVICE_NAME = BOMC)(FAILOVER_MODE=(TYPE=session)(METHOD=basic)(RETRIES=4)(DELAY=1))))(DESCRIPTION=(ADDRESS_LIST =(LOAD_BALANCE=OFF)(FAILOVER=ON)(ADDRESS = (PROTOCOL = TCP)(HOST =20.26.90.24)(PORT = 1521)))(CONNECT_DATA =(SERVICE_NAME = BOMC)(FAILOVER_MODE=(TYPE=session)(METHOD=basic)(RETRIES=4)(DELAY=1)))))";
    // 定义数据库的链接
    private static Connection connection;
    // 定义sql语句的执行对象
    private Statement statement;
    // 定义查询返回的结果集合
    private ResultSet result;
    public JDBCUtil()
    {
        try
        {
            new OracleDriver();
            connection = getConnection();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }



    // 定义获得数据库的链接
    public static Connection getConnection()
    {
        try
        {
            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return connection;
    }

    public int execute(String sql) throws SQLException{
        statement = connection.createStatement();
        int result=statement.executeUpdate(sql);
        return result;
    }

    /**
     * 定制查询SQL，支持count查询
     * @param sql
     * @throws Exception
     */
    public int selectCount(String sql) throws Exception {
        PreparedStatement pstmt = connection.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery();
        String result="0";
        while (rs.next()) {
            result = rs.getString("COUNT");
        }
        rs.close();
        pstmt.close();
        return Integer.parseInt(result);
    }

    public List<Map<String,String>> getData(String sql) {
        Map<String,String> map = null;
        List<Map<String,String>> list = null;
        try {
            statement = connection.createStatement();
            list = new ArrayList<Map<String,String>>();
            ResultSet result = statement.executeQuery(sql);
            ResultSetMetaData data = result.getMetaData();
            int count = data.getColumnCount();
            while(result.next()){
                map = new HashMap<String,String>();
                for(int i=1;i<=count;i++){
                    map.put(data.getColumnName(i), result.getString(i));
                }
                list.add(map);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public void commit(){
        try {
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void rollback(){
        try {
            connection.rollback();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void shutdown()
    {
        if (result != null)
        {
            try
            {
                result.close();
            }
            catch (SQLException e)
            {
                e.printStackTrace();
            }
        }
        if (statement != null)
        {
            try
            {
                statement.close();
            }
            catch (SQLException e)
            {
                e.printStackTrace();
            }
        }
        if (connection != null)
        {
            try
            {
                connection.close();
            }
            catch (SQLException e)
            {
                e.printStackTrace();
            }
        }
    }
}
