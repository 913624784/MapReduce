package TestMapReduce;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TestMapper {
    /**
     * Mapper 类 用于整理数据
     */
    //设置mapper的四个泛型 设置map输出的键值类型 重写map方法
    public static class Formapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        //map要输出的键值 为了写入context
        private Text okey=new Text();
        private IntWritable ovalue=new IntWritable();
        @Override
        //Hadoop中的数据类型和java不一样 text代表string 其他都是加writable
        //设置map中的键和值的数据类型 context是内容 可以进行流的操作
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                System.out.println(key);
                String line =value.toString();//把值tostring
                String str [] =line.split("\t");//把值的内容筛选出来
                if(str.length==3){//过滤掉不满足条件的记录条 对符合格式的字符串存到数组
                    //  context.write(new Text(str[0]),new IntWritable(Integer.parseInt(str[2])));
                    //str[0] 和 str[2] 是为了过滤学生学号这一项
                    okey.set(str[0]);//设置键
                    ovalue.set(Integer.parseInt(str[2]));//设置值 因为值是string 用integer包装类取数值
                    context.write(okey,ovalue);//在文件中写入值和键
                }
        }
        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            //设置一个工作实例
            Job job= Job.getInstance();
            job.setMapperClass(Formapper.class);//设置mapper类
            job.setMapOutputValueClass(IntWritable.class);//设置map输出值类型
            job.setMapOutputKeyClass(Text.class);//设置map输出的key类型
            //下面这两个是设置ruduce的键值类型 如果没有ruduce  可以不用设置
            job.setOutputValueClass(IntWritable.class);//设置输出值类型
            job.setOutputKeyClass(Text.class);//设置输出键类型
            //设置输入目录
            FileInputFormat.setInputPaths(job,new Path("E:\\class.txt"));//设置读取目录
            FileOutputFormat.setOutputPath(job,new Path("E://out1"));//设置输出目录
            job.waitForCompletion(true);//提交任务
        }








    }
}
