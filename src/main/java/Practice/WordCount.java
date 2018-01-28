package Practice;

import Util.JobUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCount {
    public  static  class Formapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        private Text okey=new Text();
        private IntWritable ovalue=new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str []= value.toString().split(" ");
            for(String s:str){
                okey.set(s);
                context.write(okey,ovalue);
            }
        }
    }

    public static class Forreducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable ovalue=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable i:values){
                sum++;
            }
            ovalue.set(sum);
            context.write(key,ovalue);
        }
    }

    /**
     *
     * @param (args 0 input  1 output)*path
     */
        public static void main(String[] args) {
          // JobUtil.commitJob(WordCount.class,"E:\\forTestData\\forWordCount","");
            JobUtil.commitJob(WordCount.class,args[0],args[1]);


        }


}
