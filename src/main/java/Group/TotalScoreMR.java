package Group;


import Util.JobUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class TotalScoreMR {
    public static class Formapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private Text okey=new Text();
        private IntWritable ovalue=new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
               String strs  []  =value.toString().split(" ");
               okey.set(strs[0]);
               ovalue.set(Integer.parseInt(strs[1]));
               context.write(okey,ovalue);

        }
    }
    public static class Forreducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable ovalue=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable i:values){
                sum+=i.get();
            }
            ovalue.set(sum);
            context.write(key,ovalue);
        }
    }

    public static void main(String[] args) throws IOException {
        /*Configuration config=new Configuration();
        config.setBoolean(Job.MAP_OUTPUT_COMPRESS,true);
        config.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC,GzipCodec.class, CompressionCodec.class);*/

        JobUtil.commitJob(TotalScoreMR.class,
                "E:\\forTestData\\gpinput",
                "",
                new MyClassTextComparator(),new GzipCodec());//自定义压缩格式
       /* FileOutputFormat.setCompressOutput(job,true);
        FileOutputFormat.setOutputCompressorClass(job,);*/

    }
}
