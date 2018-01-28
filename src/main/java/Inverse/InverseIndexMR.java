package Inverse;

import Util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class InverseIndexMR {
    public static class  Formapper extends Mapper<LongWritable,Text,Text,Text>{
        private Text okey=new Text();
        private Text ovalue=new Text();//为了ForCombiner方便
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit=(FileSplit) context.getInputSplit();//获得读取的文件
            String filename= fileSplit.getPath().getName();//获取文件名
            //把每个单词存到数组里
            String  strs [] = value.toString().split(" ");//StringUtils.split(value.toString(),' ');
            for(String s:strs){
                okey.set(s+" "+filename);//单词和文件名连接
                context.write(okey,ovalue);
            }
        }
    }
    //输入输出类型一样
    public static class ForCombiner extends Reducer<Text,Text,Text,Text> {
        private Text okey=new Text();
        private Text ovalue=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for(Text text:values){
                count++;
            }
            String strs []=key.toString().split(" ");//把键切开
            String forKey=strs[0];//还是单词
            String forValue=strs[1]+"-->"+count;//把文件和次数拼在一起
            okey.set(forKey);
            ovalue.set(forValue);
            context.write(okey,ovalue);
        }
    }
    public static class Forreducer extends Reducer<Text,Text,Text,Text>{
        private Text okey=new Text();
        private Text ovalue=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder  sb=new StringBuilder();
            for(Text text:values){
                sb.append(text.toString()+"\t");//把文件名拼起来存到sb中
            }

            okey.set(key.toString()+":");
            ovalue.set(sb.toString());
            context.write(okey,ovalue);
        }
    }

    public static void main(String str [] ){
        JobUtil.commitJob(InverseIndexMR.class,
                "E:\\forTestData\\inverseIndex\\data",
                "",
                new ForCombiner());
    }
}
