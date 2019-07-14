package com.jia.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * @author tanjia
 * @email 378097217@qq.com
 * @date 2019/7/15 2:04
 */
public class JobFactory {
    public static final Job create(final Class driver, final Class mapper, final Class reducer, final Class mapperKey, final Class mapperValue, final Class outKey, final Class outValue) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("HADOOP_HOME", "D:\\hadoop\\winutils\\hadoop-2.6.0");
        System.setProperty("hadoop.home.dir", "D:\\hadoop\\winutils\\hadoop-2.6.0");
        final Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://123.207.9.164:8020");
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        configuration.set("dfs.replication", "1");
        final Job job = Job.getInstance(configuration);
        job.setJarByClass(driver);
        job.setMapperClass(mapper);
        job.setReducerClass(reducer);
        job.setCombinerClass(reducer);
        job.setMapOutputKeyClass(mapperKey);
        job.setMapOutputValueClass(mapperValue);
        job.setOutputKeyClass(outKey);
        job.setOutputValueClass(outValue);
        return job;
    }
}
