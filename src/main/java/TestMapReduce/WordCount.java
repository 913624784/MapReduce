package TestMapReduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class WordCount {
    public static class ForMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        //map要输出的键值 为了写入context
        private Text okey = new Text();
        private IntWritable ovalue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str[] = value.toString().split(" ");
            for (String s : str) {
                okey.set(s);
                context.write(okey, ovalue);
            }
        }
    }

    public static class Forreduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        //reduce要输出的值 为了写入context
        private IntWritable ovalue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) {
                sum++;
            }
            ovalue.set(sum);
            context.write(key, ovalue);
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setMapperClass(ForMapper.class);//设置map类
        job.setReducerClass(Forreduce.class);//设置reduce类
        job.setMapOutputKeyClass(Text.class);//设置map输出键
        job.setMapOutputValueClass(IntWritable.class);//设置map输出值
        job.setOutputKeyClass(Text.class);//设置reduce输出键
        job.setOutputValueClass(IntWritable.class);//设置reduce输出值
        Path path = new Path("E://out4");
        FileInputFormat.setInputPaths(job, new Path("E:\\code.txt"));
        FileSystem fs = FileSystem.get(new URI("file:///E://out4"), new Configuration());
        if (fs.exists(path)) {//如果存在
            fs.delete(path, true);
        }
        FileOutputFormat.setOutputPath(job, new Path("E://out4"));
        job.waitForCompletion(true);
    }
}
