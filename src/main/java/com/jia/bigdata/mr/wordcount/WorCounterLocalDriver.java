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
public class WorCounterLocalDriver
{
    public static void main(final String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("HADOOP_HOME", "D:\\hadoop\\winutils\\hadoop-2.6.0");
        System.setProperty("hadoop.home.dir", "D:\\hadoop\\winutils\\hadoop-2.6.0");
        final Configuration configuration = new Configuration();
        final Job job = Job.getInstance(configuration);
        job.setJarByClass((Class)WordCountDriver.class);
        job.setMapperClass((Class)WordCounterMapper.class);
        job.setReducerClass((Class)WordCounterReducer.class);
        job.setMapOutputKeyClass((Class)Text.class);
        job.setMapOutputValueClass((Class)IntWritable.class);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path[] { new Path("input") });
        final Path path = new Path("output");
        final FileSystem fileSystem = path.getFileSystem(configuration);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
        FileOutputFormat.setOutputPath(job, new Path("output"));
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 1 : 0);
    }
}
