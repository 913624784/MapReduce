package Foroutput;


import PhoneFlow.Flow;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by 代俊朴 on 2018/1/23.
 */
public class MyDBOutput extends FileOutputFormat<Flow,NullWritable> {
    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new MyDBRecordWriter();
    }
}
