package com.hzz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by hejf on 2017/4/10.
 */
public class HdfsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsUtil.class);

    private final FileSystem fileSystem;

    //设置使上传用户
    public HdfsUtil(String user) {
        FileSystem tmpFs = null;
        String cluster = "tesla-cluster";
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", "hdfs://" + cluster);
            configuration.set("dfs.nameservices", cluster);
            configuration.set("dfs.ha.namenodes." + cluster, "nn1,nn2");
            configuration.set("dfs.namenode.rpc-address." + cluster + ".nn1", "xx1:8020");
            configuration.set("dfs.namenode.rpc-address." + cluster + ".nn2", "xx2:8020");
            configuration.set("dfs.client.failover.proxy.provider." + cluster,
                    "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
            System.setProperty("HADOOP_USER_NAME", user);
            tmpFs = FileSystem.get(configuration);
        } catch (IOException e) {
            LOG.error("初始化hadoop fileSystem出错" + e.getMessage(), e);
            throw new Error("初始化失败" + e.getMessage(), e);
        }
        fileSystem = tmpFs;
    }

    public void copyFromLocal(String src, String dst) {
        try {
            fileSystem.copyFromLocalFile(new Path(src), new Path(dst));
        } catch (IOException e) {
            throw new RuntimeException("上传文件失败：" + e.getMessage(), e);
        }
    }
}
