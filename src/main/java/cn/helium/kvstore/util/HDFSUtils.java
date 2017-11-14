package cn.helium.kvstore.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

/**
 * Copyright @ 2017 unknown.yuzhouwan.com
 * All right reserved.
 * Function: HDFS Utils
 *
 * @author Grace Koo
 * @since 2017/11/5 0011
 */
public class HDFSUtils {

    public HDFSUtils() {
    }

    public void addFile(String source, String dest, Configuration conf) throws IOException {

        FileSystem fileSystem = FileSystem.get(conf);
        String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
        if (dest.charAt(dest.length() - 1) != '/') {
            dest = dest + "/" + filename;
        } else {
            dest = dest + filename;
        }
        Path path = new Path(dest);
        if (fileSystem.exists(path)) {
            System.out.println("File " + dest + " already exists!");
            return;
        }
        FSDataOutputStream out = fileSystem.create(path);
        InputStream in = new BufferedInputStream(new FileInputStream(new File(source)));

        byte[] b = new byte[1024];
        int numBytes;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }
        in.close();
        out.close();
        fileSystem.close();
    }

    public void readFile(String file, String hdfs, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        Path path = new Path(hdfs);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + hdfs + " does not exists!");
            return;
        }
        File localFile = new File(file);
        localFile.deleteOnExit();
        localFile.createNewFile();
        OutputStream out = new BufferedOutputStream(new FileOutputStream(localFile));

        byte[] b = new byte[1024];
        int numBytes;
        FSDataInputStream in = fileSystem.open(path);
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }
        in.close();
        out.close();
        fileSystem.close();
    }

    public void deleteFile(String file, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path(file);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exists");
            return;
        }
        fileSystem.delete(new Path(file), true);
        fileSystem.close();
    }
}
