package Join;

import Util.ClearOutPut;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ForReduceJoinMR {
    public static class Formapper extends Mapper<LongWritable, Text, Text, Info> {
        private Text okey = new Text();
        private Info ovalue = new Info();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String fields[] = line.split("\t");
            if (fields.length < 3) {//过滤不合理数据  例如名称id什么的
                return;
            }
            //获得当前读到的是哪个文件 ，用来决定向 实体中保存哪些属性
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();//获取文件名
            if ("order.txt".equals(fileName)) {//当前读到的order的数据文件
                // 设置order 部分的内容
                okey.set(fields[3]);//id
                ovalue.setOrderId(Integer.parseInt(fields[0]));//编号
                ovalue.setOrderNum(Integer.parseInt(fields[2]));//数量
                ovalue.setOrderTime(fields[1]);//日期
                ovalue.setOrderpid(Integer.parseInt(fields[3]));//order表商品id
                ovalue.setFlag(true);  // 标记
            } else if ("product.txt".equals(fileName)) {
                // 读到的product的文件
                okey.set(fields[0]);//id
                ovalue.setPrice(Integer.parseInt(fields[3]));//价格
                ovalue.setProductId(fields[0]);//id
                ovalue.setProductName(fields[1]);//名称
                ovalue.setFlag(false);
            } else {
                try {
                    throw new Exception("文件读取错误异常！！！！！");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            //根据不同的文件设置ovalue 混合id相同的商品
            context.write(okey, ovalue);
        }
    }

    public static class Forreducer extends Reducer<Text, Info, Info, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Info> values, Context context) throws IOException, InterruptedException {
            String joinType = context.getConfiguration().get("demo.join");
            List<Info> orderList = new ArrayList<>();
            List<Info> productList = new ArrayList<>();
            for (Info info : values) {//此时的info是混乱的 混搭的
                if (info.isFlag()) {// order
                    orderList.add(info.clone());//装入 order 集合
                } else { // product
                    productList.add(info.clone());
                }
            }

            if (joinType.equals("IN")) {
                if (orderList.size() <= productList.size()) {
                    for (Info order : orderList) {
                        for (Info list : productList) {
                            order.setProductName(list.getProductName());
                            order.setProductId(list.getProductId());
                            order.setPrice(list.getPrice());
                            context.write(order, NullWritable.get());
                        }
                    }
                } else {
                    for (Info list : productList) {
                        for (Info order : orderList) {
                            list.setOrderNum(order.getOrderNum());
                            list.setOrderId(order.getOrderId());
                            list.setOrderTime(order.getOrderTime());
                            context.write(list, NullWritable.get());
                        }
                    }
                }
            } else if (joinType.equals("LEFT")) {//product
                for (Info list : productList) {
                    for (Info order : orderList) {
                        list.setOrderId(order.getOrderId());
                        list.setOrderNum(order.getOrderNum());
                        list.setOrderTime(order.getOrderTime());
                        context.write(list, NullWritable.get());
                    }
                    if (list.getOrderId() == 0) {
                        list.setOrderId(0);
                        list.setOrderNum(0);
                        list.setOrderTime(null);
                        context.write(list, NullWritable.get());
                    }
                }
            } else if (joinType.equals("RIGHT")) {//order
                for (Info order : orderList) {
                    for (Info list : productList) {
                        order.setPrice(list.getPrice());
                        order.setProductId(list.getProductId());
                        order.setProductName(list.getProductName());
                        context.write(order, NullWritable.get());
                    }
                    if (order.getPrice() == 0) {
                        order.setPrice(0);
                        order.setProductId(String.valueOf(order.getOrderpid()));
                        order.setProductName(null);
                        context.write(order, NullWritable.get());
                    }
                }
            } else {
                throw new RuntimeException("joinTypes is wrong");
            }

        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.getConfiguration().set("demo.join", "LEFT");
        job.setMapperClass(Formapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Info.class);
        job.setReducerClass(Forreducer.class);
        job.setOutputKeyClass(Info.class);
        job.setOutputValueClass(NullWritable.class);
        String path = "E://output";
        FileInputFormat.setInputPaths(job, new Path("E://jionData//reduce"));
        ClearOutPut.clear(path);
        FileOutputFormat.setOutputPath(job, new Path(path));
        job.waitForCompletion(true);
    }

}
