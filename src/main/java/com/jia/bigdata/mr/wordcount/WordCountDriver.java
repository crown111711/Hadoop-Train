package com.jia.bigdata.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * @author tanjia
 * @email 378097217@qq.com
 * @date 2019/7/15 2:09
 */
public class WordCountDriver
{
    public static void main(final String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("HADOOP_HOME", "D:\\hadoop\\winutils\\hadoop-2.6.0");
        System.setProperty("hadoop.home.dir", "D:\\hadoop\\winutils\\hadoop-2.6.0");
        final Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://123.207.9.164:8020");
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        configuration.set("dfs.replication", "1");
        final Job job = Job.getInstance(configuration);
        job.setJarByClass((Class)WordCountDriver.class);
        job.setMapperClass((Class)WordCounterMapper.class);
        job.setReducerClass((Class)WordCounterReducer.class);
        job.setCombinerClass((Class)WordCounterReducer.class);
        job.setMapOutputKeyClass((Class)Text.class);
        job.setMapOutputValueClass((Class)IntWritable.class);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)IntWritable.class);
        final Path path = new Path("/hdfsApi2/test/wordCounterMapper");
        final FileSystem fileSystem = path.getFileSystem(configuration);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
        FileInputFormat.setInputPaths(job, new Path[] { new Path("/hdfsApi2/test/newtest3.txt") });
        FileOutputFormat.setOutputPath(job, new Path("/hdfsApi2/test/wordCounterMapper"));
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 1 : 0);
    }
}
