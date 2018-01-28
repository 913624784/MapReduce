package Combiner;

import Util.JobUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TestAvg {
   public static class Formapper extends Mapper<LongWritable,Text,Text,AvgEntity> {
       private Text okey =new Text();
       private AvgEntity avgEntity=new AvgEntity();
       @Override
       protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           String line=value.toString();
           String [] strs=line.split(" ");
           okey.set(strs[0]);
           //设置实体
           avgEntity.setCount(1);//count为都为1
           avgEntity.setSum(Integer.parseInt(strs[1]));
           //写入
           context.write(okey,avgEntity);
       }
   }

    public static class Forcombiner extends Reducer<Text,AvgEntity,Text,AvgEntity>{
        @Override
        protected void reduce(Text key, Iterable<AvgEntity> values, Context context) throws IOException, InterruptedException {
            int count=0;
            int sum=0;
            for(AvgEntity i:values){//有几个实体就count++
                count++;
                sum+=i.getSum();//获取同一日期的实体相加
            }
            AvgEntity avgEntity=new AvgEntity();
            avgEntity.setCount(count);//同一日期实体总数 不同文件会有相同的日期
            avgEntity.setSum(sum);//同一日期温度总数
            context.write(key,avgEntity);
        }
    }
    public static class Forreducer extends Reducer<Text,AvgEntity,Text,IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<AvgEntity> values, Context context) throws IOException, InterruptedException {
            int count=0;
            int sum=0;
            for(AvgEntity avgEntity:values){
                count+=avgEntity.getCount();//同一实体的总数
                sum+=avgEntity.getSum();//同一实体的总温度
            }
            context.write(key,new IntWritable(sum/count));
        }
    }

    public static void main(String[] args) {
        JobUtil.commitJob(TestAvg.class,"E:\\forTestData\\forCombiner","",new Forcombiner());
    }

}
