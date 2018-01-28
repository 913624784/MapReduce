package PhoneFlow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Flow implements Writable {
    private int upflow;
    private int lowflow;
    private int allflow;

    public Flow() {
    }

    public Flow(int upflow, int lowflow, int allflow) {
        this.upflow = upflow;

        this.lowflow = lowflow;
        this.allflow = allflow;
    }

    @Override

    public String toString() {
        return
                "upflow=" + upflow +
                        ", lowflow=" + lowflow +
                        ", allflow=" + allflow
                ;
    }

    public int getUpflow() {
        return upflow;
    }

    public void setUpflow(int upflow) {
        this.upflow = upflow;
    }

    public int getLowflow() {
        return lowflow;
    }

    public void setLowflow(int lowflow) {
        this.lowflow = lowflow;
    }

    public int getAllflow() {
        return allflow;
    }

    public void setAllflow(int allflow) {
        this.allflow = allflow;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(upflow);
        out.writeInt(lowflow);
        out.writeInt(allflow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upflow = in.readInt();
        this.lowflow = in.readInt();
        this.allflow = in.readInt();
    }
}
