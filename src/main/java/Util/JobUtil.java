package Util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 将包含mapper等内部类的class对象传入第一个参数
 * 就可以自动的获取key 和value等类型
 * 要求 Mapper类的类名要包含Mapper字样 'Reducer' 类的类名要包含 'Reducer'字样
 */
public class JobUtil {
    /**
     * 自动提交任务的工具方法
     *
     * @param mr     包含
     * @param input  输入路径
     * @param output 输出路径 如果传入 null 或者 "" 空串则默认 为项目编译目录下的/output目录
     * @param obj    动态参数可以传入partitioner ， combiner 等对象
     */
    @SuppressWarnings("unchecked")
    public static void commitJob(Class<?> mr, String input, String output, Object... obj) {
        String path = mr.getClass().getResource("/").getPath();
        if (output == null || output.equals("")) {
            output = "E:/output";
        }
        //---------------------------------------------------------------------------------------------------------------------
        Class reduceClass = null;
        Class mapClass = null;
        Class[] classes = mr.getClasses();
        for (Class cs : classes) {
            if (cs.getTypeName().contains("mapper")) {
                mapClass = cs;
            }
            if (cs.getTypeName().contains("reducer")) {
                reduceClass = cs;
            }
        }

//----------------------------------------------------------------------------------------------------------------------
        try {
            Job job;
            //检查判断是否传入了Configuration
            if (haveConfig(obj) == null) {
                job = Job.getInstance();
            } else {
                job = Job.getInstance(haveConfig(obj));
            }

            job.setJarByClass(mr);//设置j有主方法的那个类
//----------------------------------------------------------------------------------------------------------------------
            if (mapClass != null) {
                job.setMapperClass(mapClass);
                ParameterizedType pt = ((ParameterizedType) mapClass.getGenericSuperclass());
                //获得map class 中的 out key 和out value
                System.out.println(pt.getActualTypeArguments()[2].toString().substring(6));
                System.out.println(pt.getActualTypeArguments()[3].toString().substring(6));
                job.setMapOutputKeyClass(Class.forName(pt.getActualTypeArguments()[2].toString().substring(6)));
                job.setMapOutputValueClass(Class.forName(pt.getActualTypeArguments()[3].toString().substring(6)));
            } else {
                throw new RuntimeException("not set Mapper !!! " +
                        " the mapper class name must contains 'mapper' " +
                        " the reducer class name must contains 'reducer' and " +
                        "the  rapper class must 'public'  ");
            }
            if (reduceClass != null) {
                job.setReducerClass(reduceClass);
                ParameterizedType pt = ((ParameterizedType) reduceClass.getGenericSuperclass());
                //获得 reduce class 中的out key 和value
                job.setOutputKeyClass(Class.forName(pt.getActualTypeArguments()[2].toString().substring(6)));
                job.setOutputValueClass(Class.forName(pt.getActualTypeArguments()[3].toString().substring(6)));
            }
//----------------------------------------------------------------------------------------------------------------------
            /**
             *  可以根据需求进行添加功能
             */
            for (Object o : obj) {
                if (o instanceof Partitioner) {  // set partitioner
                    job.setPartitionerClass((Class<? extends Partitioner>) o.getClass());
                }
                if (o instanceof Integer) {
                    job.setNumReduceTasks((int) o);
                }
                if (o instanceof Reducer) { // set combiner
                    job.setCombinerClass((Class<? extends Reducer>) o.getClass());
                }
                if (o instanceof URI) { // add addCacheFile
                    job.addCacheFile((URI) o);
                }
                // 如果这个类的父类是WritableComparator 说明要设置的是GroupCompara
                if (o.getClass().getSuperclass().equals(WritableComparator.class)) {
                    job.setGroupingComparatorClass((Class<? extends WritableComparator>) o.getClass());
                }
                //如果这个类的父类的父类是WritableComparator说明要设置的SortComparator
                if (o.getClass().getSuperclass().getSuperclass() != null && o.getClass().getSuperclass().getSuperclass().equals(WritableComparator.class)) { // set comparator
                    job.setSortComparatorClass((Class<? extends RawComparator>) o.getClass());
                }
                if (o instanceof InputFormat) {
                    job.setInputFormatClass((Class<? extends InputFormat>) o.getClass());
                }
                if (o instanceof DefaultCodec) {
                    FileOutputFormat.setCompressOutput(job, true);
                    FileOutputFormat.setOutputCompressorClass(job, (Class<? extends CompressionCodec>) o.getClass());
                }
                if (o instanceof OutputFormat) {
                    job.setOutputFormatClass((Class<? extends OutputFormat>) o.getClass());
                }
            }


//----------------------------------------------------------------------------------------------------------------------
            FileInputFormat.setInputPaths(job, new Path(input));
            FileSystem fileSystem = FileSystem.get(new URI("file://" + output), new Configuration());
            if (fileSystem.exists(new Path(output))) {
                fileSystem.delete(new Path(output), true);
            }
            FileOutputFormat.setOutputPath(job, new Path(output)); //输出路径
            job.waitForCompletion(true);

        } catch (IOException | ClassNotFoundException | InterruptedException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    /**
     * 检查动态参数里传入Configuration了没有
     *
     * @param obs commitJob的动态参数
     * @return null 没有传入 如果有则返回该Configuration
     */
    private static Configuration haveConfig(Object... obs) {
        for (Object o : obs) {
            if (o instanceof Configuration) {
                return (Configuration) o;
            }
        }
        return null;
    }
}
