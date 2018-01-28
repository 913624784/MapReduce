package Counter;

import Util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TestCounter {
    public static class Formapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //组 和 名
            Counter  sensitiveWord=context.getCounter("oracle","senWord");
            Counter  notNumber=context.getCounter("oracle","not number");
            String line=value.toString();//统计每一行
            if(line.contains("the")){//含有the
                sensitiveWord.increment(1L);//sensitiveWord计数器加一
            }
            Counter lineNum= context.getCounter(LineCounter.NUM);//用枚举统计api中的行号
            lineNum.increment(1L);
            try {
                Integer.parseInt(line);//api中没有int 肯定为行数
            }catch (NumberFormatException e){
                notNumber.increment(1L);//notNumber计数器加一
            }
        }
    }
    public static enum  LineCounter {//枚举相当于一个计数器
        NUM
    }

    public static void main(String[] args) {
        //ClearOutPut.clear("E:\\output");
        JobUtil.commitJob(TestCounter.class,"E:\\forTestData\\javaApi","");
    }
}
