package Join;

import Util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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
        Map<String,String> cacheMap=new HashMap<>();//缓存表 小表
        private Text okey=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String strs [] = line.split("\t");
            String pn=cacheMap.get(strs[2]);
            okey.set(line+"\t"+pn);
            context.write(okey, NullWritable.get());
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI cacheURI= context.getCacheFiles()[0];
            BufferedReader br=new BufferedReader(new FileReader(new File(cacheURI)));
            String temp;
            while((temp=br.readLine())!=null){
                String strs  []  =temp.split("\t");
                cacheMap.put(strs[0],strs[1]);
            }
        }
    }
    public static void main(String[] args) throws IOException, URISyntaxException {
        JobUtil.commitJob(ForMapJoinMR.class,"E:\\forTestData\\jionData\\map\\userinfo.txt","",new URI("file:///E://forTestData//jionData//map//phoneinfo.txt"));
    }
}

