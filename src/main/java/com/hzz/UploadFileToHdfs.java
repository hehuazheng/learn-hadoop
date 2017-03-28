package com.hzz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class UploadFileToHdfs {
    //加载默认配置
    static Configuration conf = new Configuration(true);

    /**
     *
     * java -cp .:hadoop-common-2.7.1.jar:commons-logging-1.1.3.jar:guava-11.0.2.jar:commons-collections-3.2.1.jar:commons-configuration-1.6.jar:commons-lang-2.4.jar:hadoop-auth-2.7.1.jar:slf4j-api-1.7.10.jar:slf4j-log4j12-1.7.10.jar:log4j-1.2.12.jar:hadoop-hdfs-2.7.1.jar:htrace-core-3.1.0-incubating.jar:servlet-api-2.5.jar:commons-cli-1.2.jar:protobuf-java-2.5.0.jar:commons-io-2.4.jar UploadFileToHdfs abc /user/pcsjob  xxx
     *
     * @param filePath
     * @param dst
     * @throws IOException
     */
    public static void uploadFile(String filePath, String dst) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(filePath);
        Path dstPath = new Path(dst);
        fs.copyFromLocalFile(false, srcPath, dstPath);
        fs.close();
    }

    public static void main(String[] args) throws IOException {
        conf.set("fs.default.name", "hdfs://"+args[2]+":8020");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        System.out.println("args: " + args[0]);
        uploadFile(args[0], args[1]);
    }
}
