package Join;

import Util.ClearOutPut;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class ForMapJoinMR {
    public static class Formapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Map<String, String> cacheMap = new HashMap<>();//缓存表 小表
        private Text okey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String strs[] = line.split("\t");
            String pn = cacheMap.get(strs[2]);
            okey.set(line + "\t" + pn);
            context.write(okey, NullWritable.get());
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI cacheURI = context.getCacheFiles()[0];
            BufferedReader br = new BufferedReader(new FileReader(new File(cacheURI)));
            String temp;
            while ((temp = br.readLine()) != null) {
                String strs[] = temp.split("\t");
                cacheMap.put(strs[0], strs[1]);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Job job = Job.getInstance();
        job.setMapperClass(Formapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setCacheFiles(new URI[]{new URI("file:///E://jionData//map//phoneinfo.txt")});
        job.setOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        String path = "E://output";
        FileInputFormat.setInputPaths(job, new Path("E://jionData//map//userinfo.txt"));
        ClearOutPut.clear(path);
        FileOutputFormat.setOutputPath(job, new Path(path));
        job.waitForCompletion(true);
    }
}

