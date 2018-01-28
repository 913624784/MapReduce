package Work1;

import Util.JobUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Temp3 {
    public static class Formapper extends Mapper<LongWritable,Text,Text,NullWritable> {
        Text okey=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            try {
                KPIBean.parse(line);
            } catch (Exception e) {
                e.printStackTrace();
            }
            String str[]=line.split(" ");
            String temp=str[6].toString();
            if(temp.contains("?")){
                int i=temp.indexOf("?");
                okey.set(str[6].substring(1,i));
            }else {
                okey.set(str[6].substring(1));
            }
            context.write(okey,NullWritable.get());
        }
    }
    public static class Forreducer extends Reducer<Text,NullWritable,Text,IntWritable> {
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
        JobUtil.commitJob(Temp3.class,
                "F:\\1703\\大数据\\ForMR0123\\MapReduce经典案例 WordCount 练习题及答案\\MapReduce经典案例 WordCount 练习题及答案\\实验数据",
                "");
    }
}
