package Practice;


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


public class WordCountApi {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        private Text okey=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            Pattern pattern=Pattern.compile("\\w+");
            Matcher matcher=pattern.matcher(line);
            while (matcher.find()){
                String word=matcher.group();//相当于substring从头到尾
                okey.set(word);
                context.write(okey, NullWritable.get());
            }
        }
    }
        public static class ForReducer extends Reducer<Text,NullWritable,Text,IntWritable>{

        private IntWritable ovalue=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            int count=0;
            for(NullWritable n:values){
                count++;
            }
            ovalue.set(count);
            context.write(key,ovalue);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
      /*  Job job= Job.getInstance();
            job.setMapperClass(Formapper.class);
            job.setReducerClass(Forreduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            String path="E://output";
            FileInputFormat.setInputPaths(job,new Path("E:\\Api.txt"));
            ClearOutPut.clear(path);
            FileOutputFormat.setOutputPath(job,new Path(path));
            job.waitForCompletion(true);*/
        JobUtil.commitJob(WordCountApi.class,args[0],args[1]);
    }
}
