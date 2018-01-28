package PhoneFlow;

import Util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URISyntaxException;

public class EveryFlow {
    public static class Formapper extends Mapper<LongWritable,Text,Text,Flow>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String str[]=line.split("\t");
            int upFlow=Integer.parseInt(str[8]);
            int downFlow=Integer.parseInt(str[9]);
            Flow flow=new Flow(upFlow,downFlow,upFlow+downFlow);
            context.write(new Text(str[1]),flow);
        }
    }
    public static class Forreduce extends Reducer<Text,Flow,Text,Flow>{
        private Flow flow=new Flow();
        @Override
        protected void reduce(Text key, Iterable<Flow> values, Context context) throws IOException, InterruptedException {
            int sumUP=0;
            int sumDow=0;
            for(Flow f:values){
                sumUP+=f.getUpflow();
                sumDow+=f.getLowflow();
            }
            int sumAl=sumUP+sumDow;
            flow.setAllflow(sumAl);
            flow.setLowflow(sumDow);
            flow.setUpflow(sumUP);
            context.write(key,flow);
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
       /* Job job = Job.getInstance();
        job.setMapperClass(EveryFlow.Formapper.class);//设置map类
        job.setReducerClass(EveryFlow.Forreduce.class);//设置reduce类
        job.setMapOutputKeyClass(Text.class);//设置map输出键
        job.setMapOutputValueClass(Flow.class);//设置map输出值
        job.setOutputKeyClass(Text.class);//设置reduce输出键
        job.setOutputValueClass(Flow.class);//设置reduce输出值
        Path path = new Path("E://output");
        FileInputFormat.setInputPaths(job, new Path("E:\\phone.dat"));
        FileSystem fs = FileSystem.get(new URI("file:///"+path), new Configuration());
        if (fs.exists(path)) {//如果存在
            fs.delete(path, true);
        }
        FileOutputFormat.setOutputPath(job,path);
        job.waitForCompletion(true);*/
        JobUtil.commitJob(EveryFlow.class,"E:\\phone.dat","");
    }
}
