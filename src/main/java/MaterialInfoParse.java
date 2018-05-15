import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class MaterialInfoParse {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        // 获取从集群上读取文件的文件系统对象
        // 和输入流对象
        FileSystem inFs =
                FileSystem.get(
                        URI.create(args[0]), conf);
        FSDataInputStream is =
                inFs.open(new Path(args[0]));
        // 获取本地文件系统对象
        //当然这个你也可以用FileOutputStream
        LocalFileSystem outFs =
                FileSystem.getLocal(conf);
        FSDataOutputStream os =
                outFs.create(new Path(args[1]));
        byte[] buff = new byte[1024];
        int length = 0;
        while ((length = is.read(buff)) != -1) {
            os.write(buff, 0, length);
            os.flush();
        }
        System.out.println(
                inFs.getClass().getName());
        System.out.println(
                is.getClass().getName());
        System.out.println(
                outFs.getClass().getName());
        System.out.println(
                os.getClass().getName());
        os.close();
        is.close();
    }
}