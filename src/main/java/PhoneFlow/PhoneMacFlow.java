package PhoneFlow;

import Util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class PhoneMacFlow  {

     public static class Formapper extends Mapper<LongWritable,Text,Text,MacFlow>{
         private Text okey=new Text();
         private MacFlow  ovalue=new MacFlow();
         @Override
         protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line=value.toString();
                String str  []  =line .split("\t");
                okey.set(str[1]);// phone number
                int all=Integer.parseInt(str[8])+Integer.parseInt(str[9]);
                ovalue.setFlow(all);
                ovalue.setMac(str[2]);
                context.write(okey,ovalue);
         }
     }
     public static class Forreducer extends Reducer<Text,MacFlow,Text,Text> {
         private Text ovalue=new Text ();
         @Override
         protected void reduce(Text key, Iterable<MacFlow> values, Context context) throws IOException, InterruptedException {
             Map<String,Integer> map=new HashMap<>();
             for(MacFlow macFlow:values){

                 String mac=macFlow.getMac();
                 int flow=macFlow.getFlow();

                 if(map.containsKey(mac)){//如果已经有了就 取出累和
                    map.put(mac,map.get(mac)+flow);
                 }else { // 如果没有这个mac 就新添加一个
                    map.put(mac,flow);
                 }
             }
             StringBuilder sb=new StringBuilder();
             for(Map.Entry entry:map.entrySet()){
                 sb.append(entry.getKey()+":"+entry.getValue()+"\t");
             }
             ovalue.set(sb.toString());
             context.write(key,ovalue);
         }
     }
    public static void main(String[] args) {
        JobUtil.commitJob(PhoneMacFlow.class,"E:\\phone.dat","");
    }
}
