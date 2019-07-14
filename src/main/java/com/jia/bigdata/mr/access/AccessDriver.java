package com.jia.bigdata.mr.access;

import org.apache.hadoop.io.*;
import com.jia.bigdata.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.*;

/**
 * @author tanjia
 * @email 378097217@qq.com
 * @date 2019/7/15 2:09
 */
public class AccessDriver
{
    public static void main(final String[] args) throws Exception {
        final Job job = JobFactory.create(AccessDriver.class, AccessMapper.class, AccessReducer.class, Text.class, Access.class, Text.class, Access.class);
        job.setPartitionerClass((Class)AccessPartitioner.class);
        job.setNumReduceTasks(3);
        final Path path = new Path("/mr/output");
        final FileSystem fileSystem = path.getFileSystem(job.getConfiguration());
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
        FileInputFormat.setInputPaths(job, new Path[] { new Path("/mr/access.log") });
        FileOutputFormat.setOutputPath(job, path);
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 1 : 0);
    }
}
