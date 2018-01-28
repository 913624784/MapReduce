package Foroutput;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Flow implements Writable{
    private String phoneNumber;
    private int upFlow;
    private int downFlow;
    private int allFlow;

    public Flow(String phoneNumber, int upFlow, int downFlow, int allFlow) {
        this.phoneNumber = phoneNumber;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.allFlow = allFlow;
    }

    public Flow() {
    }

    @Override
    public void write(DataOutput out) throws IOException {
         out.writeInt(upFlow);
         out.writeInt(downFlow);
         out.writeInt(allFlow);
         out.writeUTF(phoneNumber);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow=in.readInt();
        this.downFlow=in.readInt();
        this.allFlow=in.readInt();
        this.phoneNumber=in.readUTF();
    }

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
        return allFlow;
    }

    public void setAllFlow(int allFlow) {
        this.allFlow = allFlow;
    }

    @Override
    public String toString() {
        return "Flow{" +
                "phoneNumber='" + phoneNumber + '\'' +
                ", upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", allFlow=" + allFlow +
                '}';
    }
}
