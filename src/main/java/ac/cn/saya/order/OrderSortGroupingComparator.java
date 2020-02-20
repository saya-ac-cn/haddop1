package ac.cn.saya.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Title: OrderSortGroupingComparator
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/15 23:24
 * @Description:
 * 自定义分组排序
 */

public class OrderSortGroupingComparator extends WritableComparator {

    public OrderSortGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 只要id相同就认为是同一个key
        // 是同一个订单id的进入到同一个reduce
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        int result;
        if (aBean.getOrderId() > bBean.getOrderId()) {
            result = 1;
        } else if (aBean.getOrderId() < bBean.getOrderId()) {
            result = -1;
        } else {
            // 返回0表示相同的key
            result = 0;
        }
        return result;
    }
}
