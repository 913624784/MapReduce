package WordCount;


import Util.ClearOutPut;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WordCountMax {
    public static class Formapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private Text okey=new Text();
        private IntWritable ovalue=new IntWritable();
        private int max;
        @Override
        protected void map(LongWritable key, Text value, Context context)  {
            String line =value.toString();
            String str [] =line.split("\t");
            String word=str[0];//为单词
            int times=Integer.parseInt(str[1]);//为单词出现的次数
            if(times>max){//max默认为0
                max=times;
                okey.set(word);
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            ovalue.set(max);//max最后肯定是最大的那个
            context.write(okey,ovalue);
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance();
        job.setMapperClass(Formapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,new Path("E:\\word"));
        String path="E://output";
        FileOutputFormat.setOutputPath(job,new Path(path));
        ClearOutPut.clear(path);
        job.waitForCompletion(true);

    }

}
