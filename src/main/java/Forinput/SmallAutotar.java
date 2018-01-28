package Forinput;

import Util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SmallAutotar {
    public static class  Formapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(value.toString());
        }
    }

    public static void main(String[] args) {

        //多个小文件占用过多map 默认只有四个
       // JobUtil.commitJob(MultipleInputPath.class,"E:\\forTestData\\data\\computer","",new SmalFileInputFormat());
        //下面用解压后的输入
        JobUtil.commitJob(MultipleInputPath.class,"E:\\forTestData\\data\\computer.tar.gz","",new SmalFileInputFormat());

    }
}
