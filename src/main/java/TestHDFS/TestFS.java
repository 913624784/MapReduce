package TestHDFS;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestFS {
    private FileSystem fs;
    private String url="hdfs://192.168.170.128:8020";
    @Before
   public void setUp() throws URISyntaxException, IOException, InterruptedException {
        fs= FileSystem.get(new URI(url),new Configuration(),"qzw");//默认是windows的主机名,我们伪装成qzw用户
        // System.out.println(fs);
        //DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_1162416679_1, ugi=qzw (auth:SIMPLE)]]
   }
   @Test
   public void testmkdir() throws IOException {
          fs.mkdirs(new Path("/forcl2"));
   }
   @Test
   //创建文件并写入数据
   public void testcreatefile() throws IOException {
        FSDataOutputStream os=fs.create(new Path("/forcl2/test.txt"));
        for(int i=0;i<3;i++){
            os.write(("hello hadoop"+i+"\n").getBytes());
        }
        os.flush();
        os.close();
   }
   @Test
   //浏览文件内容 和文件流一样
   public  void testcatfile() throws IOException {
       FSDataInputStream is=fs.open(new Path("/forcl2/test.txt"));
       byte b[]=new byte[128];
       int len;
       while((len=is.read(b))!=-1){
           System.out.println(new String(b,0,len));
       }
   }
    @Test
    //改名 换目录 rename方法里是两个路径 前者为要改的 后者为改过的
    public void testRename() throws IOException {
        fs.rename(new Path("/forcl2/test.txt"),new Path("/forcl2/qzw.txt"));
    }
    @Test
    //删除 是否级联删除 如果级联  和该目录有关的所有目录都会删除
    public void remove() throws IOException {
        fs.delete(new Path("/forcl1/"),true);
    }
    @Test
    //上传 从本地拷
    public void upload() throws IOException {
        fs.copyFromLocalFile(new Path("E:/b.txt"),new Path("/forcl/"));
    }
    @Test
    //下载 拷到本地 copy方法中也是两个路径 前者为要复制的路径 后者为复制到的路径
    public void downLoad() throws IOException {
        fs.copyToLocalFile(new Path("/forcl/b.txt"),new Path("E://copyb.txt"));
    }
    @Test
    //查看hdfs内存信息
    public void testInfo() throws IOException {
            FsStatus fsStatus=fs.getStatus(new Path("/"));
            System.out.println(fsStatus.getUsed());//使用了多大内存
            System.out.println(fsStatus.getCapacity());//一共有多大内存
            System.out.println(fsStatus.getRemaining());//还剩多少内存
    }
    @Test
    //查看文件信息
    public void testInfoA() throws IOException {
        FileStatus fileStatus=fs.getFileStatus(new Path("/forcl2/test.txt"));
        System.out.println(fileStatus.getAccessTime());//创建时间
        System.out.println(fileStatus.getReplication());//副本数
        System.out.println(fileStatus.getLen());//长度
        System.out.println(fileStatus.getOwner());//所有者
        System.out.println(fileStatus.getGroup());//组
        System.out.println(fileStatus.getBlockSize());//块大小
    }

    @Test
    //输出当前hdfs中所有用户的IP和端口信息
    public void testDS() throws IOException {
        DistributedFileSystem ds = (DistributedFileSystem) fs;
        DatanodeInfo[] dis = ds.getDataNodeStats();
        for (DatanodeInfo di : dis) {
            System.out.println(di);
        }
    }
    @Test
    public void copy() throws IOException {
        FileUtil.copyMerge(fs,new Path("/date"),fs,new Path("/ddfile"),false,new Configuration(),"");
    }
    @After
    public void tearDown(){
        try {
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
