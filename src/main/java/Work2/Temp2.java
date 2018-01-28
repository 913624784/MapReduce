package Work2;

import Util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Temp2 {


    public static class Formapper extends Mapper<LongWritable,Text,Text,Text> {
        Text okey=new Text();
        Text ovalue=new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            Weibo weibo=Weibo.util(line);
            if(!weibo.getUserid().equals("")) {
                okey.set(weibo.getUserid());
                ovalue.set(weibo.getCreationdate());
                context.write(okey,ovalue);
            }
        }
    }
    public static class Forreducer extends Reducer<Text,Text,Text,Text> {
        Text okey=new Text();
        Text ovalue=new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count=0;
            String max=null;
            String min=null;
            for(Text t:values){
                count++;
                if(max==null||max.compareTo(t.toString())<0){
                    max=t.toString();
                }
                if(min==null||min.compareTo(t.toString())>0){
                    min=t.toString();
                }
            }
            okey.set(key);
            ovalue.set("count:"+count+"\t max:"+max+"\t min:"+min);
            context.write(okey,  ovalue);
        }
    }

    public static void main(String[] args) {
        JobUtil.commitJob(Temp2.class,
                "F:\\1703\\大数据\\ForMR0123\\MapReduce基础编程 练习题及答案\\MapReduce基础编程 练习题及答案\\MapReduce基础——网站微博数据统计分析\\数据"
                ,"",new Text.Comparator());
    }

}
