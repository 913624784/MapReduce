package PhoneFlow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MacFlow implements Writable {
    private  String   mac ;
    private  int  flow;

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public int getFlow() {
        return flow;
    }

    public void setFlow(int flow) {
        this.flow = flow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
            out.writeInt(flow);
            out.writeUTF(mac);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.flow=in.readInt();
        this.mac=in.readUTF();
    }

    @Override
    public String toString() {
        return "MacFlow{" +
                "mac='" + mac + '\'' +
                ", flow=" + flow +
                '}';
    }
}
