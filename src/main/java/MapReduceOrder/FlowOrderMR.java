package MapReduceOrder;

import Util.ClearOutPut;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowOrderMR {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,FlowOrder>{
        FlowOrder ovalue=new FlowOrder();
        Text okey=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String str  []  =  line.split("\t");
            ovalue.setUpFlow(Integer.parseInt(str[8]));
            ovalue.setDownFlow(Integer.parseInt(str[9]));
            okey.set(str[1]);
            context.write(okey,ovalue);
        }
    }

    public static class ForReducer extends Reducer<Text,FlowOrder,FlowOrder,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<FlowOrder> values, Context context) throws IOException, InterruptedException {
            int allUp=0;
            int allDown=0;
            for(FlowOrder  fo:values){
                allDown+=fo.getDownFlow();
                allUp+=fo.getUpFlow();
            }
            FlowOrder flowOrder=new FlowOrder();
            flowOrder.setDownFlow(allDown);
            flowOrder.setUpFlow(allUp);
            flowOrder.setAllFlow(flowOrder.getAllFlow());
            flowOrder.setPhoneNumber(key.toString());
            context.write(flowOrder,NullWritable.get());
        }
    }
    //map输出的类型
    public static class MyPartitioner extends Partitioner<Text,FlowOrder> {

        @Override
        public int getPartition(Text text, FlowOrder flowOrder, int numPartitions) {
            String phoneNumber=text.toString();
            String numberPart=phoneNumber.substring(0,3);
            if("135".equals(numberPart)){
                return 1;
            }
            if("136".equals(numberPart)){
                return 2;
            }
            if("137".equals(numberPart)){
                return 3;
            }
            return 0;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job=Job.getInstance();
        job.setMapperClass(FlowOrderMR.ForMapper.class);
        job.setReducerClass(FlowOrderMR.ForReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowOrder.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowOrder.class);
        job.setPartitionerClass(MyPartitioner.class);//设置分区机制
        job.setNumReduceTasks(4);//设置ruduce个数
        FileInputFormat.setInputPaths(job,new Path("E://phone.dat"));
        FileOutputFormat.setOutputPath(job,new Path("E://output"));
        ClearOutPut.clear("E://output");
        job.waitForCompletion(true);
    }
}
