package Work2;

import Util.JobUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Temp1 {
    public static class Formapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        Text okey=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           String line=value.toString();
           Weibo weibo=Weibo.util(line);
           if(!weibo.getPostid().equals("")) {
               okey.set(weibo.getPostid());
               context.write(okey,NullWritable.get());
           }
        }
    }
    public static class Forreducer extends Reducer<Text,NullWritable,Text,IntWritable>{
        IntWritable ovalue=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for(NullWritable nullWritable:values){
                count++;
            }

            ovalue.set(count);
            context.write(key,ovalue);
        }
    }

    public static void main(String[] args) {
        JobUtil.commitJob(Temp1.class,
                "F:\\1703\\大数据\\ForMR0123\\MapReduce基础编程 练习题及答案\\MapReduce基础编程 练习题及答案\\MapReduce基础——网站微博数据统计分析\\数据"
                ,"");
    }

}
