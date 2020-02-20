package ac.cn.saya.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Title: OrderBean
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/15 23:01
 * @Description:
 */

public class OrderBean implements WritableComparable<OrderBean> {

    private int orderId;// 订单号
    private double price;// 价格

    public OrderBean() {
    }

    public OrderBean(int orderId, double price) {
        this.orderId = orderId;
        this.price = price;
    }

    public void set(int orderId, double price) {
        this.orderId = orderId;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        int result;
        /**
         * 根据订单进行升序排序，订单相同，按价格降序
         */
        if (orderId > o.getOrderId()) {
            result = 1;
        } else if (orderId < o.getOrderId()) {
            result = -1;
        } else {
            result = price > o.getPrice() ? -1 : 1;
        }
        return result;
    }

    /**
     * 序列化
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(orderId);
        out.writeDouble(price);
    }

    /**
     * 反序列化
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        orderId = in.readInt();
        price = in.readDouble();
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
