package Join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Info implements Writable, Cloneable {
    private int orderId;//编号
    private String orderTime = "";//日期
    private int orderNum;//数量
    private int orderpid;//order表的商品id
    //=============================
    private String productId = "";//如果为空 不会序列化失败
    private String productName = "";//名称
    private int price;//价格

    private boolean flag;//true 为order 控制表是哪一个

    @Override
    protected Info clone() {
        try {
            return (Info) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(orderId);
        out.writeUTF(orderTime);
        out.writeInt(orderNum);
        out.writeInt(orderpid);
        out.writeUTF(productId);
        out.writeUTF(productName);
        out.writeInt(price);
        out.writeBoolean(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readInt();
        this.orderTime = in.readUTF();
        this.orderNum = in.readInt();
        this.orderpid = in.readInt();
        this.productId = in.readUTF();
        this.productName = in.readUTF();
        this.price = in.readInt();
        this.flag = in.readBoolean();
    }

    public int getOrderpid() {
        return orderpid;
    }

    public void setOrderpid(int orderpid) {
        this.orderpid = orderpid;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public String getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(String orderTime) {
        this.orderTime = orderTime;
    }

    public int getOrderNum() {
        return orderNum;
    }

    public void setOrderNum(int orderNum) {
        this.orderNum = orderNum;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public boolean isFlag() {
        return flag;
    }

    /**
     * true 代表order
     *
     * @param flag
     */
    public void setFlag(boolean flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return "Info{" +
                "orderId=" + orderId +
                ", orderTime='" + orderTime + '\'' +
                ", orderNum=" + orderNum +
                ", productId='" + productId + '\'' +
                ", productName='" + productName + '\'' +
                ", price=" + price +
                '}';
    }
}
