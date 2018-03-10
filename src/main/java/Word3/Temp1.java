package Word3;

import Util.JobUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

import java.io.IOException;
import java.util.List;

public class Temp1 {
    public static class Formapper extends Mapper<LongWritable,Text,Text,NullWritable> {
        Text okey=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        List<Word> words=WordSegmenter.segWithStopWords(value.toString());
        for(Word i:words){
            System.out.println(i);
            okey.set(i.toString());
            context.write(okey,NullWritable.get());
        }
        }
    }
    public static class Forreducer extends Reducer<Text,NullWritable,Text,IntWritable> {
        private IntWritable ovalue=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(NullWritable i:values){
                sum++;
            }
            ovalue.set(sum);
            context.write(key,ovalue);
        }
    }
    public static void main(String[] args) {
        JobUtil.commitJob(Temp1.class,"F:\\tt.txt","");
    }
}
