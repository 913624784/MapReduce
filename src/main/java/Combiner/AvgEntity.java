package Combiner;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgEntity implements Writable{
    private int count;
    private int sum;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        out.writeInt(sum);
    }

    @Override
    public String toString() {
        return "AvgEntity{" +
                "count=" + count +
                ", sum=" + sum +
                '}';
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.count=in.readInt();
        this.sum=in.readInt();
    }
}
