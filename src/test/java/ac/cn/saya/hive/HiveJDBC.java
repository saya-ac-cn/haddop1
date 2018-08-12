package ac.cn.saya.hive;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveJDBC {

    /**
     * 查询班级信息
     */
    @Test
    public void queryClass()
    {
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        try
        {
            //获取连接
            con = HiveJDBCUtils.getConnection();
            //创建运行环境
            st = con.createStatement();
            //要执行的hql语句
            String hql = "select * from classes";
            //执行hql
            rs = st.executeQuery(hql);
            //循环读取数据
            while (rs.next())
            {
                //取出班级ID、专业ID、班级名
                System.out.println(rs.getInt(0)+"\t"+rs.getInt(1)+"\t"+rs.getString(2));
            }
        }catch (Exception e)
        {
            System.err.println(e.getMessage());
        }finally {
            //及时释放掉资源
            HiveJDBCUtils.close(con,st,rs);
        }
    }

}
