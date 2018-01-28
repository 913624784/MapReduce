package MapReduceOrder;

import Util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ForStringOrderMR {
    public static class Formapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(value.toString());
            context.write(value,NullWritable.get());
        }
    }
    public static class Forreduce extends Reducer<Text,NullWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println(key.toString());
            context.write(key,NullWritable.get());
        }
    }

    public static class MyTextComparator extends Text.Comparator{
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            String str1=new String (b1,s1+1,l1-1);
            String str2=new String(b2,s2+1,l2-1);
            System.out.println(str1+","+str2);
            if(str1.length()==str2.length()){
                int  a=Integer.parseInt(str1);
                int  b=Integer.parseInt(str2);
                return b-a;
            }
            return str1.length()-str2.length();
        }
    }
    public static void main(String[] args) {
        JobUtil.commitJob(ForStringOrderMR.class,"E:/py.txt","",new MyTextComparator());
    }
}
