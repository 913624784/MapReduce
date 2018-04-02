package Wordcount;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordTimes implements WritableComparable<WordTimes> {//自定义序列化和比较器
    private String word="";
    private int times;

    public WordTimes(String word, int times) {
        this.word = word;
        this.times = times;
    }

    public WordTimes() {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.writeInt(times);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.word=in.readUTF();
        this.times=in.readInt();
    }

    @Override
    public int compareTo(WordTimes o) {
        return o.times-this.times;//按照次数比较 从大到小
    }

    @Override
    public String toString() {
        return "WordTimes{" +
                "word='" + word + '\'' +
                ", times=" + times +
                '}';
    }


}

