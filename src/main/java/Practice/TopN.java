package Practice;

import Util.ClearOutPut;
import WordCount.WordCountTopN;
import WordCount.WordTimes;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TopN {
    public static class Formapper extends Mapper<LongWritable,Text, WordTimes,NullWritable>{
        private List<WordTimes> list=new ArrayList();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String str[] =line.split("\t");
            WordTimes wordTimes=new WordTimes(str[0],Integer.parseInt(str[1]));
            list.add(wordTimes);
            Collections.sort(list);
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(int i=0;i<10;i++){//获得前十个 写进去 因为重写了比较器 是从大到小
                context.write(list.get(i), NullWritable.get());
            }
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance();
        job.setMapperClass(WordCountTopN.ForMapper.class);
        job.setMapOutputKeyClass(WordTimes.class);
        job.setMapOutputValueClass(NullWritable.class);
        String path="E://output";
        FileInputFormat.setInputPaths(job,new Path("E:\\word"));
        ClearOutPut.clear(path);
        FileOutputFormat.setOutputPath(job,new Path(path));
        job.waitForCompletion(true);


    }
}
