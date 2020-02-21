package ac.cn.saya.table.reduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Title: TableBean
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/17 23:33
 * @Description:
 */

public class TableBean  implements Writable {

    private String orderId; // 订单 id
    private String pId; // 产品 id
    private int amount; // 产品数量
    private String pname; // 产品名称
    private String flag;// 表的标记

    public TableBean() {
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return orderId + '\t' +  pname + "\t" + amount + "\t" ;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(orderId);
        output.writeUTF(pId);
        output.writeInt(amount);
        output.writeUTF(pname);
        output.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.orderId = input.readUTF();
        this.pId = input.readUTF();
        this.amount = input.readInt();
        this.pname = input.readUTF();
        this.flag = input.readUTF();
    }
}
