package PhoneFlow;

import Util.JobUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URISyntaxException;

public class AllFlow {
    public static class Formapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private Text okey=new Text();
        private IntWritable ovalue=new IntWritable();
        private int lowflow;
        private int upflow;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String str[] =line.split("\t");
            upflow=Integer.parseInt(str[8]);
            lowflow=Integer.parseInt(str[9]);
            int allflow=upflow+lowflow;
            okey.set(str[1]);
            ovalue.set(allflow);
            context.write(okey,ovalue);
        }
    }
    public static class Forreduce extends Reducer<Text,IntWritable,Text,IntWritable>{
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

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException {
       /* Job job = Job.getInstance();
        job.setMapperClass(Formapper.class);//设置map类
        job.setReducerClass(Forreduce.class);//设置reduce类
        job.setMapOutputKeyClass(Text.class);//设置map输出键
        job.setMapOutputValueClass(IntWritable.class);//设置map输出值
        job.setOutputKeyClass(Text.class);//设置reduce输出键
        job.setOutputValueClass(IntWritable.class);//设置reduce输出值
        Path path = new Path("E://output");
        FileInputFormat.setInputPaths(job, new Path("E:\\phone.dat"));
        FileSystem fs = FileSystem.get(new URI("file:///"+path), new Configuration());
        if (fs.exists(path)) {//如果存在
            fs.delete(path, true);
        }
        FileOutputFormat.setOutputPath(job,path);
        job.waitForCompletion(true);*/
        JobUtil.commitJob(AllFlow.class,"E:\\phone.dat","");
    }

}
