package Forinput;

import Util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MultipleInputPath {
    public static class  Formapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(key.toString()+","+value.toString());
             // System.out.println(value.toString());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /* Job job= Job.getInstance();
         job.setMapperClass(ForMapper.class);
         job.setMapOutputKeyClass(Text.class);
         job.setMapOutputValueClass(Text.class);
      // job.setInputFormatClass(KeyValueTextInputFormat.class);//用指定的input键值
        FileInputFormat.setInputPaths(job,new Path("E:\\forTestData\\friend"),
                new Path("E:\\forTestData\\forClass"));
        FileOutputFormat.setOutputPath(job,new Path("E:/output1"));
        job.waitForCompletion(true);*/

        //Configuration configuration=new Configuration();
        //该配置设置后  达到分片大小即分片
        //configuration.setLong("mapreduce.input.fileinputformat.split.maxsize",409600);
        JobUtil.commitJob(MultipleInputPath.class,"E:\\forTestData\\testkeyValue","");

    }
}
