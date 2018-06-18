package Forinput;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

public class SmalFileInputFormat extends CombineFileInputFormat<Text,Text> {

    @Override//修改为false 取消默认分片规则
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    @Override//重写createRecordReader方法 用一个类继承RecordReader 返回他
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader((CombineFileSplit) split,context,SmalRecordReader.class);
    }
}
