package MapReduceOrder;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowOrder implements WritableComparable<FlowOrder>{
    private String phoneNumber="";
    private int upFlow;
    private int downFlow;
    private int allFlow;

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getAllFlow() {
        return this.downFlow+this.upFlow;
    }

    public void setAllFlow(int allFlow) {
        this.allFlow = allFlow;
    }

    /**
     * 二次排序的规则！！！
     * @param o
     * @return
     */
    @Override
    public int compareTo(FlowOrder o) {
        if(o.allFlow==this.allFlow){
            return o.downFlow-this.downFlow;
        }
        return o.allFlow-this.allFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phoneNumber);
        out.writeInt(upFlow);
        out.writeInt(downFlow);
        out.writeInt(allFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.phoneNumber=in.readUTF();
        this.upFlow=in.readInt();
        this.downFlow=in.readInt();
        this.allFlow=in.readInt();

    }

    @Override
    public String toString() {
        return   phoneNumber + "\t" + upFlow +
                "\t" + downFlow +
                "\t" + allFlow;
    }
}
