package MapReduceOrder;

import Util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowOrderMRTwo {
    public static class Formapper extends Mapper<LongWritable,Text,FlowOrder,NullWritable>{
        FlowOrder ovalue=new FlowOrder();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String str  []  =  line.split("\t");
            ovalue.setUpFlow(Integer.parseInt(str[1]));
            ovalue.setDownFlow(Integer.parseInt(str[2]));
            ovalue.setAllFlow(ovalue.getAllFlow());
            ovalue.setPhoneNumber(str[0]);
            context.write(ovalue,NullWritable.get());
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        JobUtil.commitJob(FlowOrderMRTwo.class,"E:\\phone1","");
    }
}
