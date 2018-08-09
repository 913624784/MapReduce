package Forinput;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

public class SmalRecordReader extends RecordReader<Text,Text> {

    private Text key = new Text();
    private Text value = new Text();
    private boolean isFinish;//返回当前状态 是否完成分片
    private CombineFileSplit split;//**
    private FSDataInputStream inputStream;
    private TaskAttemptContext context;//**
    private Integer currentIndex;//**

    public SmalRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer currentIndex) {
        this.split = split;
        this.context = context;
        this.currentIndex = currentIndex;
    }
    //指定一个空的构造器
    public SmalRecordReader() {

    }

    @Override//初始化
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

    }

    /**
     * 对 key value 进行赋值
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!isFinish) {//如果true
            Path path = split.getPath(currentIndex);
            String fileName = path.getName();
            key.set(fileName);// 把文件名作为key
            FileSystem fs = path.getFileSystem(context.getConfiguration());
            inputStream = fs.open(path);
            byte content[] = new byte[(int) split.getLength(currentIndex)];//把整个文件装到byte数组里
            inputStream.readFully(content);
            value.set(content);//把数组里的内容装到value里
            isFinish = true;
            return true;
        }
        return false;
    }

    /**
     * 返回key
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * 返回value
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     *
     * 返回进度
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isFinish ? 1 : 0;
    }

    @Override
    public void close() throws IOException {

    }
}
