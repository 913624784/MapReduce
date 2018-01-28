package WordCount;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

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

    public static void main(String[] args) {
        Set<WordTimes> set=new TreeSet();
        set.add(new WordTimes("c",20));
        set.add(new WordTimes("a",1));
        set.add(new WordTimes("d",4));
        set.add(new WordTimes("b",2));
        System.out.println(set);
    }
}

