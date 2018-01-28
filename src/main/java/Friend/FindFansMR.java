package Friend;

import Util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FindFansMR {
    public static class Formapper extends Mapper<LongWritable,Text,Text,Text> {
        private Text okey=new Text();
        private Text ovalue=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String strs [] =value.toString().split(":");
            String forV=strs[0];//前面的人 本人
            String friends [] =strs[1].split(",");//他关注的人
            for(String s: friends){
                okey.set(s);
                ovalue.set(forV);
                context.write(okey,ovalue);//他和他关注的人
            }
        }
    }
    public static class Forreducer extends Reducer<Text,Text,Text,Text> {
        private Text okey=new Text();
        private Text ovalue=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            okey.set(key.toString()+":");//他加个：就行
            StringBuilder sb=new StringBuilder();//拼接
            for(Text text:values){
                sb.append(text.toString()+",");//每一个粉丝 中间用，隔开
            }
            ovalue.set(sb.toString());
            context.write(okey,ovalue);
        }
    }

    public static void main(String[] args) {
        JobUtil.commitJob(FindFansMR.class,"E:\\forTestData\\friend","");
    }
}
