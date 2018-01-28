import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        String s="  <row Id=\"191\" PostId=\"101\" Text=\"I, a WordPress user, am very interested in astronomy.\" CreationDate=\"2010-09-03T18:48:56.677\" UserId=\"172\" />\n";
       // String xml[]=s.trim().substring(5, s.trim().length() - 3).split("\"");
        String xml[]=s.trim().split("\"");
        List<String> list = new ArrayList();
        if(xml.length>=11&&xml[xml.length-3].equals(" UserId=")) {
            if(xml[4].equals(" Text=")){//没有score

            }
            int i = 1;
            int k = 0;
            while (i < xml.length) {
                list.add(xml[i]);
                i = i + 2;
            }
        }
      for(String ss:xml)
          System.out.println(ss);
    }
}
