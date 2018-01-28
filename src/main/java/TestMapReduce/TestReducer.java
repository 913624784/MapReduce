package TestMapReduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestReducer {
    /**
     * Mapper 类 用于整理数据
     */
    //设置reduce的四个泛型 设置map输出的键值类型 重写map方法
    public static class Formapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        //map要输出的键值 为了写入context
        private Text okey = new Text();
        private IntWritable ovalue = new IntWritable();

        @Override
        //Hadoop中的数据类型和java不一样 text代表string 其他都是加writable
        //设置map中的键和值的数据类型 context是内容 可以进行流的操作
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(key);
            String line = value.toString();//把值tostring
            String str[] = line.split("\t");//把值的内容筛选出来
            if (str.length == 3) {//过滤掉不满足条件的记录条
                //  context.write(new Text(str[0]),new IntWritable(Integer.parseInt(str[2])));
                //str[0] 和 str[2] 是为了过滤学生学号这一项
                okey.set(str[0]);//设置键
                ovalue.set(Integer.parseInt(str[2]));//设置值 因为值是string 用integer包装类取数值
                context.write(okey, ovalue);//在文件中写入值和键
            }
        }
    }
    /**
     * reduce类用于计算数据
     */
    //设置reduce的四个泛型 要和mapper键值对应  设置reduce输出的键值类型 重写reduce方法
    public static class ForReducer extends Reducer<Text,IntWritable,Text,Text> {
        //reduce要输出的值 为了写入context
        private Text okey=new Text();
        @Override
        //Iterable是可迭代的
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable i:values){
                sum+=i.get();
            }
            //结果应该为 班级编号,总分、
            String result="班级编号："+key.toString();
            okey.set(result);
            context.write(okey,new Text(", 总分"+sum));
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //设置 Mapper的类reduce的类 及 map的输出类型 reducer的输出类型
        Job job= Job.getInstance();
        Path path=new Path("E://out1");
        job.setMapperClass(Formapper.class);//设置mapper类
        job.setReducerClass(ForReducer.class);//设置reduce类
        job.setMapOutputKeyClass(Text.class);//设置map的key
        job.setMapOutputValueClass(IntWritable.class);//设置map的值
        job.setOutputKeyClass(Text.class);//设置输reduce出键类型
        job.setOutputValueClass(Text.class);//设置reduce输出值类型
        //设置输入目录
        FileInputFormat.setInputPaths(job,new Path("E:\\class.txt"));//设置读取目录
        FileSystem fs= FileSystem.get(new URI("file:///E://out1"),new Configuration());
        if(fs.exists(path)){//如果存在
            fs.delete(path,true);
        }
        FileOutputFormat.setOutputPath(job,new Path("E://out1"));//设置输出目录
        job.waitForCompletion(true);//提交任务
    }





}
