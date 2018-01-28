package Util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class ClearOutPut {

    public static void clear(String path) {
        Path output = new Path(path);
        try {
            FileSystem fs = FileSystem.get(new URI("file://" + path), new Configuration());
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
