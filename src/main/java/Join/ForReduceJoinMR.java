package Join;


import Util.JobUtil;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
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
                ovalue.setFlag(true);  // 标记 位设置好
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
            Info product = new Info();
            List<Info> orderList = new ArrayList<>();
            for (Info info : values) {//此时的info是混乱的 混搭的
                try {
                    if (info.isFlag()) {// order
                        Info order = new Info();
                        //  复制相同属性的工具方法 Apache提供的 复制上面的有的属性 相当于get后又set
                        BeanUtils.copyProperties(order, info);
                        orderList.add(order);//装入 order 集合
                    } else { // product
                        BeanUtils.copyProperties(product, info);
                    }
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
            for (Info order : orderList) {
                order.setProductName(product.getProductName());
                order.setProductId(product.getProductId());
                order.setPrice(product.getPrice());
                context.write(order, NullWritable.get());
            }


        }
    }

    public static void main(String[] args) {
        JobUtil.commitJob(ForReduceJoinMR.class,"E:\\forTestData\\jionData\\reduce","");
    }

}
