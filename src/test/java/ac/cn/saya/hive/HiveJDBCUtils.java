package ac.cn.saya.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * hive连接工具
 */
public class HiveJDBCUtils {

    private static String driver = "org.apache.hive.jdbc.HiveDriver";
    private static String  url = "jdbc:hive2://120.132.116.155:10000/myhive?useUnicode=true&characterEncoding=UTF-8";

    // 注册驱动
    static {
            try {
                Class.forName(driver);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
    }

    /**
     * 获取连接
     * @return
     */
    public static Connection getConnection(){
        try {
            return DriverManager.getConnection(url,"root","Lnk742442849");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 释放资源
     * @param con
     * @param st
     * @param rs
     */
    public static void close(Connection con,Statement st,ResultSet rs)
    {
        /**
         * 注意关闭对象的先后顺序
         */
        if(rs != null)
        {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                rs = null;
            }
        }
        if(st != null)
        {
            try {
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                st = null;
            }
        }
        if(con != null)
        {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                con = null;
            }
        }
    }

}
