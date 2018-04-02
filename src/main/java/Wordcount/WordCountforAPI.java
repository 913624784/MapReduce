package Wordcount;

import Util.JobUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCountforAPI {
    public static class Formapper extends Mapper<LongWritable,Text,Text,NullWritable> {
        private Text okey = new Text();

        Formapper() {//一个文件会执行一次构造器
            System.err.println("===============================================");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Pattern pattern = Pattern.compile("\\w+");//将给定的正则表达式编译到模式中 统计单词的频数
            Matcher matcher = pattern.matcher(line);//一行一行匹配line
            while (matcher.find()) {
                //对于具有输入序列 s 的匹配器 m，表达式 m.group() 和 s.substring(m.start(), m.end()) 是等效的。
                String word = matcher.group();//返回由以前匹配操作所匹配的输入子序列
                okey.set(word);//把匹配到的单词放到键里
                context.write(okey, NullWritable.get());//给reduce键(单词) 不给值
            }
        }
    }
        public static class Forreducer extends Reducer<Text,NullWritable,Text,IntWritable> {
            private IntWritable ovalue=new IntWritable();
            @Override
            protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
                int sum=0;
                for(NullWritable nullWritable:values){//遍历每种单词有多少个
                    sum++;
                }
                ovalue.set(sum);//每个单词的个数
                context.write(key,ovalue);//对应的单词有多少个
            }
        }
        public static void main(String[] args)  {
           /*Job job= Job.getInstance();
            job.setMapperClass(Formapper.class);
            job.setReducerClass(Forreducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            String path="E://output";
            FileInputFormat.setInputPaths(job,new Path("E:\\Api.txt"));
            ClearOutPut.clear(path);
            FileOutputFormat.setOutputPath(job,new Path(path));
            job.waitForCompletion(true);*/
            JobUtil.commitJob(WordCountforAPI.class,"E:\\forTestData\\javaApi","");

        }


}
