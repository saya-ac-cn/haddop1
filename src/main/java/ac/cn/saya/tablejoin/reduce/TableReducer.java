package ac.cn.saya.tablejoin.reduce;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @Title: TableReducer
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/17 23:53
 * @Description:
 */

public class TableReducer  extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        // 1 准备存储订单的集合
        ArrayList<TableBean> orderList = new ArrayList<>();

        // 2 准备 bean 对象
        TableBean pdBean = new TableBean();

        for (TableBean bean:values)
        {
            if("0".equals(bean.getFlag()))
            {
                // 来自订单表

                // 拷贝传递过来的每条数据到集合中
                TableBean orderBean = new TableBean();
                try {
                    BeanUtils.copyProperties(orderBean, bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                // 放入到集合中
                orderList.add(orderBean);
            }
            else
            {
                //  产品表
                try {
                    // 拷贝传递过来的产品表到内存中
                    BeanUtils.copyProperties(pdBean, bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        // 3 表的拼接
        for(TableBean bean:orderList){
            // 写入品牌
            bean.setPname (pdBean.getPname());
            // 4 数据写出去
            context.write(bean, NullWritable.get());
        }

    }
}
